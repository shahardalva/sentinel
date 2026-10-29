// Package store keeps tenants and their client keys in Postgres, with pgx.
// It stores only SHA-256 digests of keys; ActiveDigests serves them to
// keys.Refreshing as a keys.Fetch. Every error it returns is sanitized
// (see Error): none holds the connection string, user, password, host or
// database name. It never logs, and never stores or logs a plaintext key.
package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maxNameLen bounds a tenant name, in bytes.
const maxNameLen = 200

// Tenant is a customer of the gateway; its keys work while it is enabled.
type Tenant struct {
	ID         string
	Name       string
	CreatedAt  time.Time
	DisabledAt *time.Time // nil while enabled
}

// Key is a client key's metadata. The key itself is never stored; Prefix
// is its first 12 characters, to tell keys apart.
type Key struct {
	ID        string
	TenantID  string
	Prefix    string
	CreatedAt time.Time
	RevokedAt *time.Time // nil while active
}

// Store is a connection pool to the key database. It is safe for
// concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the database at connString and checks it answers. It
// doesn't migrate; call Migrate. pgx also reads the libpq environment
// variables (PGHOST, PGPASSWORD...) and ~/.pgpass for anything connString
// leaves out. The error names neither connString nor any part of it.
func Open(ctx context.Context, connString string) (*Store, error) {
	cfg, err := poolConfig(connString)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, sanitize("open", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, sanitize("open", err)
	}
	return &Store{pool: pool}, nil
}

// MaxConns is the pool's size, set whatever the connection string says
// (its pool_max_conns is overridden) rather than pgx's default of
// max(4, NumCPU), so the numbers below hold on any machine. A gateway's
// users of the pool, which it shares between them:
//
//   - the admin API: at most admin.MaxInFlight (MaxConns/2 = 4) at once,
//     one connection per request;
//   - the key refresh: one connection per fetch, every
//     SENTINEL_KEYS_REFRESH;
//   - Migrate: one, at startup, before either of the others.
//
// So with the admin API full, 4 connections are still free for the
// refresh, which needs 1, while Postgres answers. When it doesn't, an
// admin call that times out frees its admin slot at once, but pgx keeps
// its connection for up to 15 s more while it cancels the query and
// cleans up (a CancelRequest on a new connection, then a drain), so
// repeated timeouts can hold more than 4 and leave the refresh waiting.
// The refresh is failing then anyway; a pool of its own would make the
// guarantee unconditional. Close waits for those cleanups too, so
// shutdown can take up to about 15 s longer after such a timeout. 8 per
// gateway keeps 12 gateways under Postgres's default max_connections of
// 100.
const MaxConns = 8

// poolConfig parses connString into a pool config of MaxConns
// connections. The error names no part of connString.
func poolConfig(connString string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		// pgx's parse errors quote the connection string, password masked
		// but user and host not.
		return nil, &Error{Op: "open", desc: "invalid connection string"}
	}
	cfg.MaxConns = MaxConns
	return cfg, nil
}

// Close closes every connection, waiting for those in use.
func (s *Store) Close() { s.pool.Close() }

// CreateTenant adds an enabled tenant. A name in use is ErrDuplicateName;
// an empty or overlong name, one with surrounding whitespace, or one with
// a character that isn't printable or is a format character (Cf), is
// ErrInvalidName.
func (s *Store) CreateTenant(ctx context.Context, name string) (Tenant, error) {
	if !validName(name) {
		return Tenant{}, ErrInvalidName
	}
	var t Tenant
	err := s.pool.QueryRow(ctx,
		`INSERT INTO tenants (name) VALUES ($1) RETURNING id, name, created_at, disabled_at`,
		name).Scan(&t.ID, &t.Name, &t.CreatedAt, &t.DisabledAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "tenants_name_key" {
			return Tenant{}, ErrDuplicateName
		}
		return Tenant{}, sanitize("create tenant", err)
	}
	return t, nil
}

// ListTenants returns every tenant, disabled ones too, oldest first.
func (s *Store) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, created_at, disabled_at FROM tenants ORDER BY created_at, id`)
	if err != nil {
		return nil, sanitize("list tenants", err)
	}
	ts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Tenant, error) {
		var t Tenant
		err := r.Scan(&t.ID, &t.Name, &t.CreatedAt, &t.DisabledAt)
		return t, err
	})
	if err != nil {
		return nil, sanitize("list tenants", err)
	}
	return ts, nil
}

// DisableTenant disables a tenant: ActiveDigests leaves out all its keys
// from then on, and it gets no new ones. Disabling a disabled tenant keeps
// the first time. An unknown ID is ErrNotFound.
func (s *Store) DisableTenant(ctx context.Context, id string) error {
	if !validUUID(id) {
		return ErrNotFound
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE tenants SET disabled_at = coalesce(disabled_at, now()) WHERE id = $1`, id)
	if err != nil {
		return sanitize("disable tenant", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// CreateKey issues a new key for an enabled tenant. It returns the key's
// metadata and the key itself, which is not stored and can't be shown
// again; the caller hands it to the client and must not log it. An unknown
// tenant is ErrNotFound, a disabled one ErrTenantDisabled.
func (s *Store) CreateKey(ctx context.Context, tenantID string) (Key, string, error) {
	const op = "create key"
	if !validUUID(tenantID) {
		return Key{}, "", ErrNotFound
	}
	key, prefix, digest := newKey()
	// FOR SHARE waits for a DisableTenant in progress on the row, then
	// re-checks disabled_at, so no key is issued to a tenant being disabled.
	// The foreign key's own lock (FOR KEY SHARE) doesn't conflict with that
	// update.
	var k Key
	err := s.pool.QueryRow(ctx,
		`INSERT INTO api_keys (tenant_id, prefix, digest)
		 SELECT id, $2, $3 FROM tenants WHERE id = $1 AND disabled_at IS NULL FOR SHARE
		 RETURNING id, tenant_id, prefix, created_at, revoked_at`,
		tenantID, prefix, digest[:]).Scan(&k.ID, &k.TenantID, &k.Prefix, &k.CreatedAt, &k.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		disabled, err := s.tenantDisabled(ctx, op, tenantID)
		if err != nil {
			return Key{}, "", err
		}
		if disabled {
			return Key{}, "", ErrTenantDisabled
		}
		// The insert found no enabled tenant, yet it is enabled now. Nothing
		// re-enables a tenant, so this can't happen; don't guess.
		return Key{}, "", &Error{Op: op, desc: "tenant changed during the call"}
	}
	if err != nil {
		return Key{}, "", sanitize(op, err)
	}
	return k, key, nil
}

// ListKeys returns a tenant's keys, revoked ones too, oldest first: their
// metadata only. An unknown tenant is ErrNotFound.
func (s *Store) ListKeys(ctx context.Context, tenantID string) ([]Key, error) {
	const op = "list keys"
	if !validUUID(tenantID) {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, tenant_id, prefix, created_at, revoked_at FROM api_keys
		 WHERE tenant_id = $1 ORDER BY created_at, id`, tenantID)
	if err != nil {
		return nil, sanitize(op, err)
	}
	ks, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Key, error) {
		var k Key
		err := r.Scan(&k.ID, &k.TenantID, &k.Prefix, &k.CreatedAt, &k.RevokedAt)
		return k, err
	})
	if err != nil {
		return nil, sanitize(op, err)
	}
	if len(ks) == 0 {
		// No keys, or no such tenant.
		if _, err := s.tenantDisabled(ctx, op, tenantID); err != nil {
			return nil, err
		}
	}
	return ks, nil
}

// RevokeKey revokes a key: ActiveDigests leaves it out from then on.
// Revoking a revoked key keeps the first time. An unknown ID is
// ErrNotFound.
func (s *Store) RevokeKey(ctx context.Context, id string) error {
	if !validUUID(id) {
		return ErrNotFound
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at = coalesce(revoked_at, now()) WHERE id = $1`, id)
	if err != nil {
		return sanitize("revoke key", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ActiveDigests returns the SHA-256 digests of every key that is not
// revoked and whose tenant is not disabled. It is a keys.Fetch: it honors
// ctx, and its errors are safe to log.
func (s *Store) ActiveDigests(ctx context.Context) ([][sha256.Size]byte, error) {
	const op = "active digests"
	rows, err := s.pool.Query(ctx,
		`SELECT k.digest FROM api_keys k JOIN tenants t ON t.id = k.tenant_id
		 WHERE k.revoked_at IS NULL AND t.disabled_at IS NULL`)
	if err != nil {
		return nil, sanitize(op, err)
	}
	digests, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) ([sha256.Size]byte, error) {
		var d [sha256.Size]byte
		var b []byte
		if err := r.Scan(&b); err != nil {
			return d, err
		}
		if len(b) != sha256.Size {
			// The schema's check makes this impossible; refuse rather than
			// serve a truncated digest.
			return d, &Error{Op: op, desc: "digest of the wrong length"}
		}
		copy(d[:], b)
		return d, nil
	})
	if err != nil {
		return nil, sanitize(op, err)
	}
	return digests, nil
}

// tenantDisabled reports whether a tenant is disabled, or ErrNotFound.
func (s *Store) tenantDisabled(ctx context.Context, op, id string) (bool, error) {
	var disabled bool
	err := s.pool.QueryRow(ctx,
		`SELECT disabled_at IS NOT NULL FROM tenants WHERE id = $1`, id).Scan(&disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, sanitize(op, err)
	}
	return disabled, nil
}

// validName reports whether name can be a tenant's name.
func validName(name string) bool {
	if name == "" || len(name) > maxNameLen || !utf8.ValidString(name) || strings.TrimSpace(name) != name {
		return false
	}
	return !strings.ContainsFunc(name, func(r rune) bool {
		// IsPrint rules out control characters and every space but U+0020.
		// Format characters (bidi overrides, zero-width characters) could
		// make one tenant's name look like another's in a listing.
		return !unicode.IsPrint(r) || unicode.Is(unicode.Cf, r)
	})
}

// validUUID reports whether id is a UUID in its canonical text form, so a
// malformed ID is ErrNotFound rather than a database error.
func validUUID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F') {
				return false
			}
		}
	}
	return true
}
