package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockKey is the pg_advisory_xact_lock key every gateway takes
// before reading or changing the schema: "sentinel" in ASCII.
const migrationLockKey int64 = 0x73656e74696e656c

// rollbackTimeout bounds the rollback after a failed migration, which runs
// even once ctx is done.
const rollbackTimeout = 5 * time.Second

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
// at a version newer than this binary knows is ErrSchemaTooNew, and one
// whose applied migrations differ from this binary's by name is
// ErrSchemaMismatch; either way nothing is changed. A migration that fails
// is rolled back whole.
func (s *Store) Migrate(ctx context.Context) error {
	return s.migrate(ctx, migrationFS)
}

// migrate applies the migrations in fsys; tests pass their own.
func (s *Store) migrate(ctx context.Context, fsys fs.FS) error {
	ms, err := loadMigrations(fsys)
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
//
// The transaction is READ COMMITTED whatever the server's or role's
// default, because the runner relies on it: every statement after the lock
// must see what the gateway that held the lock before committed. Under
// REPEATABLE READ or SERIALIZABLE the snapshot would be taken by the lock
// statement, before the wait, and a gateway that waited would read the old
// version and run a migration again.
func (s *Store) applyNext(ctx context.Context, ms []migration) (applied bool, err error) {
	const op = "migrate"
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, sanitize(op, err)
	}
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()
		_ = tx.Rollback(rctx)
	}()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		return false, sanitize(op, err)
	}
	// CREATE TABLE IF NOT EXISTS needs the CREATE privilege even when the
	// table exists, so check first: an up-to-date database then needs only
	// SELECT on schema_migrations.
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return false, sanitize(op, err)
	}
	if !exists {
		if _, err := tx.Exec(ctx, `CREATE TABLE schema_migrations (
			version    integer PRIMARY KEY,
			name       text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
			return false, sanitize(op, err)
		}
	}
	current, err := checkApplied(ctx, tx, ms)
	if err != nil {
		return false, err
	}
	if current == len(ms) {
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

// checkApplied returns the database's version after checking that its
// applied migrations are ms's first ones, by version and name.
func checkApplied(ctx context.Context, tx pgx.Tx, ms []migration) (int, error) {
	const op = "migrate"
	type applied struct {
		version int
		name    string
	}
	rows, err := tx.Query(ctx, `SELECT version, name FROM schema_migrations ORDER BY version`)
	if err != nil {
		return 0, sanitize(op, err)
	}
	as, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (applied, error) {
		var a applied
		err := r.Scan(&a.version, &a.name)
		return a, err
	})
	if err != nil {
		return 0, sanitize(op, err)
	}
	if n := len(as); n > 0 && as[n-1].version > len(ms) {
		return 0, fmt.Errorf("%w: database at version %d, binary knows up to %d", ErrSchemaTooNew, as[n-1].version, len(ms))
	}
	for i, a := range as {
		// The name read from the database is not repeated in the error.
		if a.version != i+1 || a.name != ms[i].name {
			return 0, fmt.Errorf("%w: at version %d", ErrSchemaMismatch, i+1)
		}
	}
	return len(as), nil
}

// SchemaVersion returns the latest migration version applied, 0 for a
// database never migrated.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	const op = "schema version"
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return 0, sanitize(op, err)
	}
	if !exists {
		return 0, nil
	}
	var v int
	if err := s.pool.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&v); err != nil {
		return 0, sanitize(op, err)
	}
	return v, nil
}
