package store_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/keys"
	"github.com/shahardalva/sentinel/internal/proxy"
	"github.com/shahardalva/sentinel/internal/store"
)

// Distinctive connection details, so a leak is easy to spot.
const (
	leakUser = "sntl_user_q7x"
	leakPass = "sntl_pw_z9k"
	leakDB   = "sntl_db_m4v"
)

func assertNoLeak(t *testing.T, err error, secrets ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	for _, s := range secrets {
		if strings.Contains(err.Error(), s) {
			t.Errorf("error %q contains %q", err, s)
		}
	}
}

func TestOpen_UnreachableErrorHoldsNoConnectionDetails(t *testing.T) {
	conn := "postgres://" + leakUser + ":" + leakPass + "@127.0.0.1:1/" + leakDB + "?sslmode=disable&connect_timeout=5"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := store.Open(ctx, conn)
	if s != nil {
		s.Close()
	}
	t.Logf("error: %v", err)
	assertNoLeak(t, err, leakUser, leakPass, leakDB, "127.0.0.1", ":1")
	var se *store.Error
	if !errors.As(err, &se) {
		t.Errorf("error %v is not a *store.Error", err)
	}
}

func TestOpen_BadConnStringErrorHoldsNoConnectionDetails(t *testing.T) {
	for _, conn := range []string{
		"postgres://" + leakUser + ":" + leakPass + "@db.example:notaport/" + leakDB,
		"host=db.example user=" + leakUser + " password=" + leakPass + " dbname=" + leakDB + " sslmode=bogus",
	} {
		_, err := store.Open(context.Background(), conn)
		assertNoLeak(t, err, leakUser, leakPass, leakDB, "db.example")
	}
}

// The tests below run on a real Postgres. They are skipped unless
// SENTINEL_TEST_POSTGRES_URL is set; make test-postgres and CI set it. Each
// test gets a schema of its own, dropped afterwards, so they can share a
// database and run in parallel. The migration lock is database-wide, so
// migrations across tests still serialize.

func testURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("SENTINEL_TEST_POSTGRES_URL")
	if u == "" {
		t.Skip("SENTINEL_TEST_POSTGRES_URL not set")
	}
	return u
}

// testSchema creates an empty schema and returns a connection string whose
// search_path is that schema.
func testSchema(t *testing.T) string {
	t.Helper()
	base := testURL(t)
	ctx := context.Background()
	admin, err := store.Open(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := "t_" + strings.ToLower(rand.Text())
	if err := store.ExecForTest(ctx, admin, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.ExecForTest(context.Background(), admin, `DROP SCHEMA `+schema+` CASCADE`) })
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("SENTINEL_TEST_POSTGRES_URL must be a URL: %v", err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func openMigrated(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), testSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPostgres_WrongPasswordErrorHoldsNoConnectionDetails(t *testing.T) {
	u, err := url.Parse(testURL(t))
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(leakUser, leakPass)
	u.Path = "/" + leakDB
	_, err = store.Open(context.Background(), u.String())
	assertNoLeak(t, err, leakUser, leakPass, leakDB, u.Hostname())
	var se *store.Error
	if !errors.As(err, &se) || se.Code == "" {
		t.Errorf("error %v: want a *store.Error with the server's SQLSTATE", err)
	}
}

func TestPostgres_MigrateTwiceIsANoOp(t *testing.T) {
	t.Parallel()
	s := openMigrated(t)
	ctx := context.Background()
	v1, err := s.SchemaVersion(ctx)
	if err != nil || v1 < 1 {
		t.Fatalf("SchemaVersion = %d, %v", v1, err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	v2, err := s.SchemaVersion(ctx)
	if err != nil || v2 != v1 {
		t.Fatalf("SchemaVersion after second Migrate = %d, %v; want %d", v2, err, v1)
	}
	n, err := store.CountForTest(ctx, s, `SELECT count(*) FROM schema_migrations`)
	if err != nil || n != v1 {
		t.Fatalf("schema_migrations has %d rows (%v), want %d", n, err, v1)
	}
}

func TestPostgres_ConcurrentMigrate(t *testing.T) {
	t.Parallel()
	conn := testSchema(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := range errs {
		wg.Go(func() {
			s, err := store.Open(ctx, conn) // a pool of its own, like a gateway
			if err != nil {
				errs[i] = err
				return
			}
			defer s.Close()
			errs[i] = s.Migrate(ctx)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("gateway %d: %v", i, err)
		}
	}
	s, err := store.Open(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	v, err := s.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n, err := store.CountForTest(ctx, s, `SELECT count(*) FROM schema_migrations`)
	if err != nil || n != v {
		t.Fatalf("schema_migrations has %d rows (%v), want %d: a migration ran twice", n, err, v)
	}
}

func TestPostgres_RefusesNewerSchema(t *testing.T) {
	t.Parallel()
	s := openMigrated(t)
	ctx := context.Background()
	if err := store.ExecForTest(ctx, s, `INSERT INTO schema_migrations (version, name) VALUES (9999, 'from_the_future')`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); !errors.Is(err, store.ErrSchemaTooNew) {
		t.Fatalf("Migrate = %v, want ErrSchemaTooNew", err)
	}
}

func hasDigest(t *testing.T, s *store.Store, key string) bool {
	t.Helper()
	ds, err := s.ActiveDigests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte(key))
	for _, d := range ds {
		if d == want {
			return true
		}
	}
	return false
}

func TestPostgres_KeyLifecycle(t *testing.T) {
	t.Parallel()
	s := openMigrated(t)
	ctx := context.Background()
	tn, err := s.CreateTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	k, key, err := s.CreateKey(ctx, tn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if k.TenantID != tn.ID || k.Prefix != key[:12] || k.RevokedAt != nil {
		t.Fatalf("key metadata %+v", k)
	}
	if !hasDigest(t, s, key) {
		t.Fatal("a new key is not in ActiveDigests")
	}
	ks, err := s.ListKeys(ctx, tn.ID)
	if err != nil || len(ks) != 1 || ks[0].ID != k.ID {
		t.Fatalf("ListKeys = %+v, %v", ks, err)
	}

	if err := s.RevokeKey(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if hasDigest(t, s, key) {
		t.Fatal("a revoked key is still in ActiveDigests")
	}
	if err := s.RevokeKey(ctx, k.ID); err != nil {
		t.Fatalf("revoking twice: %v", err)
	}
	ks, err = s.ListKeys(ctx, tn.ID)
	if err != nil || len(ks) != 1 || ks[0].RevokedAt == nil {
		t.Fatalf("ListKeys after revoke = %+v, %v", ks, err)
	}
}

func TestPostgres_DisableTenantDropsItsKeys(t *testing.T) {
	t.Parallel()
	s := openMigrated(t)
	ctx := context.Background()
	a, err := s.CreateTenant(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.CreateTenant(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	_, a1, _ := s.CreateKey(ctx, a.ID)
	_, a2, _ := s.CreateKey(ctx, a.ID)
	_, b1, err := s.CreateKey(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DisableTenant(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if hasDigest(t, s, a1) || hasDigest(t, s, a2) {
		t.Fatal("a disabled tenant's key is still in ActiveDigests")
	}
	if !hasDigest(t, s, b1) {
		t.Fatal("another tenant's key is gone")
	}
	if _, _, err := s.CreateKey(ctx, a.ID); !errors.Is(err, store.ErrTenantDisabled) {
		t.Fatalf("CreateKey on a disabled tenant = %v, want ErrTenantDisabled", err)
	}
	ts, err := s.ListTenants(ctx)
	if err != nil || len(ts) != 2 {
		t.Fatalf("ListTenants = %+v, %v", ts, err)
	}
	for _, tn := range ts {
		if (tn.ID == a.ID) != (tn.DisabledAt != nil) {
			t.Errorf("tenant %s DisabledAt = %v", tn.Name, tn.DisabledAt)
		}
	}
}

func TestPostgres_TypedErrors(t *testing.T) {
	t.Parallel()
	s := openMigrated(t)
	ctx := context.Background()
	if _, err := s.CreateTenant(ctx, "dup"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateTenant(ctx, "dup"); !errors.Is(err, store.ErrDuplicateName) {
		t.Errorf("duplicate name = %v, want ErrDuplicateName", err)
	}
	if _, err := s.CreateTenant(ctx, " dup"); !errors.Is(err, store.ErrInvalidName) {
		t.Errorf("bad name = %v, want ErrInvalidName", err)
	}
	const missing = "00000000-0000-4000-8000-000000000000"
	for _, id := range []string{missing, "not-a-uuid"} {
		if err := s.DisableTenant(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("DisableTenant(%q) = %v, want ErrNotFound", id, err)
		}
		if _, _, err := s.CreateKey(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("CreateKey(%q) = %v, want ErrNotFound", id, err)
		}
		if _, err := s.ListKeys(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("ListKeys(%q) = %v, want ErrNotFound", id, err)
		}
		if err := s.RevokeKey(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("RevokeKey(%q) = %v, want ErrNotFound", id, err)
		}
	}
	tn, err := s.CreateTenant(ctx, "nokeys")
	if err != nil {
		t.Fatal(err)
	}
	if ks, err := s.ListKeys(ctx, tn.ID); err != nil || len(ks) != 0 {
		t.Errorf("ListKeys of a tenant with no keys = %v, %v", ks, err)
	}
}

func TestPostgres_DigestLengthEnforced(t *testing.T) {
	t.Parallel()
	s := openMigrated(t)
	ctx := context.Background()
	tn, err := s.CreateTenant(ctx, "short")
	if err != nil {
		t.Fatal(err)
	}
	err = store.ExecForTest(ctx, s, `INSERT INTO api_keys (tenant_id, prefix, digest) VALUES ('`+tn.ID+`', 'snl_x', '\x0102')`)
	var se *store.Error
	if !errors.As(err, &se) || se.Code != "23514" {
		t.Fatalf("a 2-byte digest = %v, want a check violation (23514)", err)
	}
}

// refreshCounter is a proxy.Recorder numbering the fetches that finish,
// and remembering the number of the last that succeeded.
type refreshCounter struct{ finished, lastOK atomic.Int64 }

func (*refreshCounter) RecordRequest(context.Context, proxy.RequestInfo) {}
func (*refreshCounter) RecordAuthRejection(context.Context)              {}
func (*refreshCounter) RecordKeySetSize(context.Context, int)            {}
func (r *refreshCounter) RecordKeyRefresh(_ context.Context, ok bool) {
	n := r.finished.Add(1)
	if ok {
		r.lastOK.Store(n)
	}
}

func TestPostgres_RevokedKeyRejectedAfterOneRefresh(t *testing.T) {
	t.Parallel()
	s := openMigrated(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tn, err := s.CreateTenant(ctx, "e2e")
	if err != nil {
		t.Fatal(err)
	}
	k, key, err := s.CreateKey(ctx, tn.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Fetches run one at a time, and each one is reported after its set is
	// swapped in, so the n-th report is the n-th fetch.
	var started atomic.Int64
	rec := &refreshCounter{}
	src, err := keys.NewRefreshing(ctx, keys.Config{
		Fetch: func(ctx context.Context) ([][sha256.Size]byte, error) {
			started.Add(1)
			return s.ActiveDigests(ctx)
		},
		Interval:     200 * time.Millisecond,
		FetchTimeout: 100 * time.Millisecond,
		Recorder:     rec,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); <-src.Done() }()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errs := proxy.ErrorFormatterFunc(func(int, proxy.ErrorKind, string) (string, []byte) {
		return "text/plain", []byte("no")
	})
	h := proxy.NewAuthSource(src, logger, nil).Require(errs, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	call := func() int {
		r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		r.Header.Set("x-api-key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if got := call(); got != http.StatusOK {
		t.Fatalf("before revoke: %d, want 200", got)
	}

	if err := s.RevokeKey(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	// A fetch already running may have read the key before the revoke;
	// wait for a successful one that started after it.
	after := started.Load() + 1
	for deadline := time.Now().Add(10 * time.Second); rec.lastOK.Load() < after; {
		if time.Now().After(deadline) {
			t.Fatalf("no successful fetch after the revoke; %d finished", rec.finished.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := call(); got != http.StatusUnauthorized {
		t.Fatalf("after one refresh: %d, want 401", got)
	}
}
