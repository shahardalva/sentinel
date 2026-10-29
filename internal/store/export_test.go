package store

import (
	"context"
	"crypto/rand"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// ExecForTest runs sql on s's pool, for tests that set up or break state
// the store's own methods can't.
func ExecForTest(ctx context.Context, s *Store, sql string) error {
	_, err := s.pool.Exec(ctx, sql)
	return sanitize("exec for test", err)
}

// CountForTest runs a query returning one integer.
func CountForTest(ctx context.Context, s *Store, sql string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, sql).Scan(&n)
	return n, sanitize("count for test", err)
}

// ShowForTest returns a setting's value in a session of s's pool, as SHOW
// would.
func ShowForTest(ctx context.Context, s *Store, name string) (string, error) {
	var v string
	err := s.pool.QueryRow(ctx, `SELECT current_setting($1)`, name).Scan(&v)
	return v, sanitize("show for test", err)
}

// BeginForTest starts a transaction on s's pool, for tests that hold a
// row lock while another call runs.
func BeginForTest(ctx context.Context, s *Store) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

// LatestVersionForTest is the version of the newest embedded migration.
func LatestVersionForTest(t *testing.T) int {
	t.Helper()
	ms, err := loadMigrations(migrationFS)
	if err != nil {
		t.Fatal(err)
	}
	return ms[len(ms)-1].version
}

// The Postgres tests run on a real database. They are skipped unless
// SENTINEL_TEST_POSTGRES_URL is set; make test-postgres and CI set it. Each
// test gets a schema of its own, dropped afterwards, so they can share a
// database and run in parallel. The migration lock is database-wide, so
// migrations across tests still serialize.

// PGURLForTest returns SENTINEL_TEST_POSTGRES_URL, parsed, or skips the
// test when it is unset. It must be a postgres:// URL.
func PGURLForTest(t *testing.T) *url.URL {
	t.Helper()
	raw := os.Getenv("SENTINEL_TEST_POSTGRES_URL")
	if raw == "" {
		t.Skip("SENTINEL_TEST_POSTGRES_URL not set")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		// Not the value: it holds a password.
		t.Fatal("SENTINEL_TEST_POSTGRES_URL must be a postgres:// URL, not a key=value DSN")
	}
	return u
}

// NewSchemaForTest creates an empty schema, dropped when the test ends, and
// returns its name and a URL whose search_path is that schema.
func NewSchemaForTest(t *testing.T) (schema string, u *url.URL) {
	t.Helper()
	u = PGURLForTest(t)
	ctx := context.Background()
	admin, err := Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema = "t_" + strings.ToLower(rand.Text())
	if err := ExecForTest(ctx, admin, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ExecForTest(context.Background(), admin, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Errorf("dropping test schema %s: %v", schema, err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return schema, u
}
