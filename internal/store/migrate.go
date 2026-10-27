package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockKey is the pg_advisory_xact_lock key every gateway takes
// before reading or changing the schema: "sentinel" in ASCII.
const migrationLockKey int64 = 0x73656e74696e656c

// migration is one embedded .sql file, NNNN_name.sql.
type migration struct {
	version int
	name    string
	sql     string
}

var migrationName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// loadMigrations reads the embedded migrations. Their versions must run
// 1, 2, 3... with no gap, in file name order.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, "migrations")
	if err != nil {
		return nil, err
	}
	var ms []migration
	for _, e := range entries {
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("store: bad migration file name %q", e.Name())
		}
		v, _ := strconv.Atoi(m[1])
		if v != len(ms)+1 {
			return nil, fmt.Errorf("store: migration %q: want version %d", e.Name(), len(ms)+1)
		}
		b, err := fs.ReadFile(fsys, "migrations/"+e.Name())
		if err != nil {
			return nil, err
		}
		ms = append(ms, migration{version: v, name: m[2], sql: string(b)})
	}
	if len(ms) == 0 {
		return nil, fmt.Errorf("store: no migrations")
	}
	return ms, nil
}

// Migrate brings the schema up to date. Each migration runs in its own
// transaction, which first takes a transaction-scoped advisory lock, so
// gateways starting at once apply each migration exactly once, one after
// another. A database already at the latest version is a no-op. A database
// at a version newer than this binary knows is ErrSchemaTooNew, and nothing
// is changed.
func (s *Store) Migrate(ctx context.Context) error {
	ms, err := loadMigrations(migrationFS)
	if err != nil {
		return err
	}
	for {
		applied, err := s.applyNext(ctx, ms)
		if err != nil {
			return err
		}
		if !applied {
			return nil
		}
	}
}

// applyNext applies the first migration the database hasn't had, in one
// transaction, and reports whether there was one.
func (s *Store) applyNext(ctx context.Context, ms []migration) (applied bool, err error) {
	const op = "migrate"
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, sanitize(op, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		return false, sanitize(op, err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer PRIMARY KEY,
		name       text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return false, sanitize(op, err)
	}
	var current int
	if err := tx.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return false, sanitize(op, err)
	}
	latest := ms[len(ms)-1].version
	if current > latest {
		return false, fmt.Errorf("%w: database at version %d, binary knows up to %d", ErrSchemaTooNew, current, latest)
	}
	if current == latest {
		return false, nil
	}
	m := ms[current] // versions run from 1, so ms[current] has version current+1
	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return false, sanitize(fmt.Sprintf("%s %04d_%s", op, m.version, m.name), err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.version, m.name); err != nil {
		return false, sanitize(op, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, sanitize(op, err)
	}
	return true, nil
}

// SchemaVersion returns the latest migration version applied, 0 for none.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := s.pool.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&v)
	if err != nil {
		return 0, sanitize("schema version", err)
	}
	return v, nil
}
