package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/shahardalva/sentinel/internal/proxy"
)

func TestSanitize_PgErrorKeepsOnlyCodeAndConstraint(t *testing.T) {
	// The server's own message can name the user or database.
	pgErr := &pgconn.PgError{
		Severity:       "FATAL",
		Code:           "28P01",
		Message:        `password authentication failed for user "sntl_user_q7x"`,
		Detail:         "db sntl_db_m4v",
		ConstraintName: "",
	}
	err := sanitize("open", fmt.Errorf("wrapped by pgx at 10.1.2.3:5432: %w", pgErr))
	for _, leak := range []string{"sntl_user_q7x", "sntl_db_m4v", "10.1.2.3", "password authentication"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("error %q contains %q", err, leak)
		}
	}
	var se *Error
	if !errors.As(err, &se) || se.Code != "28P01" {
		t.Fatalf("error %v: want *Error with code 28P01", err)
	}
	if errors.Unwrap(err) != nil {
		t.Errorf("Unwrap = %v, want nil: the original error must not stay in the chain", errors.Unwrap(err))
	}
}

func TestSanitize_KeepsContextErrors(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		err := sanitize("op", fmt.Errorf("dial tcp 10.1.2.3:5432: %w", cause))
		if !errors.Is(err, cause) {
			t.Errorf("errors.Is(%v, %v) = false", err, cause)
		}
		if strings.Contains(err.Error(), "10.1.2.3") {
			t.Errorf("error %q holds the address", err)
		}
	}
}

func TestSanitize_OtherErrorsHideTheirText(t *testing.T) {
	err := sanitize("op", errors.New("host=db.internal user=admin"))
	if strings.Contains(err.Error(), "db.internal") || strings.Contains(err.Error(), "admin") {
		t.Errorf("error %q holds the original text", err)
	}
	if sanitize("op", nil) != nil {
		t.Error("sanitize(nil) != nil")
	}
	if !errors.Is(sanitize("op", ErrNotFound), ErrNotFound) {
		t.Error("a typed error lost its identity")
	}
}

func TestNewKey_Format(t *testing.T) {
	seen := map[string]bool{}
	for range 100 {
		key, prefix, digest := newKey()
		if !strings.HasPrefix(key, "snl_") {
			t.Fatalf("key %q lacks snl_", key)
		}
		body := strings.TrimPrefix(key, "snl_")
		if strings.ContainsAny(body, "=+/") {
			t.Fatalf("key %q is not unpadded base64url", key)
		}
		raw, err := base64.RawURLEncoding.DecodeString(body)
		if err != nil || len(raw) != 32 {
			t.Fatalf("key %q decodes to %d bytes (err %v), want 32", key, len(raw), err)
		}
		if len(key) != 4+43 {
			t.Fatalf("len(key) = %d, want 47", len(key))
		}
		if prefix != key[:12] || len(prefix) != 12 {
			t.Fatalf("prefix %q, want %q", prefix, key[:12])
		}
		if want := proxy.DigestKeys([]string{key})[0]; digest != want {
			t.Fatal("digest differs from proxy.DigestKeys: Auth would not accept the key")
		}
		if seen[key] {
			t.Fatal("duplicate key")
		}
		seen[key] = true
	}
}

func TestLoadMigrations_Embedded(t *testing.T) {
	ms, err := loadMigrations(migrationFS)
	if err != nil {
		t.Fatal(err)
	}
	if ms[0].version != 1 || ms[0].name != "init" {
		t.Errorf("first migration = %d %q", ms[0].version, ms[0].name)
	}
}

func TestLoadMigrations_Rejects(t *testing.T) {
	for name, tc := range map[string]struct {
		fsys fstest.MapFS
		want string
	}{
		"gap":        {fstest.MapFS{"migrations/0001_a.sql": {}, "migrations/0003_c.sql": {}}, "want version 2"},
		"bad name":   {fstest.MapFS{"migrations/1_a.sql": {}}, "bad migration file name"},
		"other file": {fstest.MapFS{"migrations/0001_a.sql": {}, "migrations/.keep": {}}, "bad migration file name"},
		"not one":    {fstest.MapFS{"migrations/0002_b.sql": {}}, "want version 1"},
		"empty dir":  {fstest.MapFS{"migrations": {Mode: fs.ModeDir}}, "no migrations"},
	} {
		_, err := loadMigrations(tc.fsys)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %v, want one containing %q", name, err, tc.want)
		}
	}
}

// Only an *Error or a bare sentinel passes through as is. Anything that
// merely wraps one could carry other text, so it is sanitized too.
func TestSanitize_PassesThroughOnlyItsOwnErrors(t *testing.T) {
	own := &Error{Op: "op", desc: "x"}
	if got := sanitize("other", own); got != own {
		t.Errorf("an *Error became %v", got)
	}
	if got := sanitize("other", ErrNotFound); got != ErrNotFound {
		t.Errorf("a bare sentinel became %v", got)
	}
	for _, wrapped := range []error{
		fmt.Errorf("host db.internal user admin: %w", ErrNotFound),
		fmt.Errorf("host db.internal user admin: %w", own),
	} {
		got := sanitize("op", wrapped)
		if strings.Contains(got.Error(), "db.internal") || strings.Contains(got.Error(), "admin") {
			t.Errorf("sanitize(%q) = %q, kept the wrapper's text", wrapped, got)
		}
	}
}

func TestValidUUID(t *testing.T) {
	for id, want := range map[string]bool{
		"0b6f1f1e-6f1e-4c1a-9b1e-1e6f1f1e6f1e": true,
		"0B6F1F1E-6F1E-4C1A-9B1E-1E6F1F1E6F1E": true,
		"0b6f1f1e6f1e4c1a9b1e1e6f1f1e6f1e":     false,
		"0b6f1f1e-6f1e-4c1a-9b1e-1e6f1f1e6f1g": false,
		"":                                     false,
		"' OR 1=1 --":                          false,
	} {
		if got := validUUID(id); got != want {
			t.Errorf("validUUID(%q) = %v, want %v", id, got, want)
		}
	}
}

func TestValidName(t *testing.T) {
	for name, want := range map[string]bool{
		"acme":                          true,
		"Acme Corp":                     true,
		"":                              false,
		" acme":                         false,
		"acme\n":                        false,
		"ac\x00me":                      false,
		"ac\x1bme":                      false,
		strings.Repeat("a", 200):        true,
		strings.Repeat("a", 201):        false,
		string([]byte{0xff, 0xfe, 'a'}): false,
		"Caf\u00e9 Z\u00fcrich":         true,
		"\u65e5\u672c":                  true,
		"acme\u202egnp":                 false, // right-to-left override
		"ac\u200bme":                    false, // zero-width space
		"ac\u2060me":                    false, // word joiner
		"ac\ufeffme":                    false, // byte order mark
		"ac\u00a0me":                    false, // no-break space, not IsPrint
		"ac\u2028me":                    false, // line separator
	} {
		if got := validName(name); got != want {
			t.Errorf("validName(%q) = %v, want %v", name, got, want)
		}
	}
}

// A migration that fails halfway leaves nothing of itself behind: the
// database stays at the version before it.
func TestMigrate_FailedMigrationAppliesNothing(t *testing.T) {
	_, u := NewSchemaForTest(t)
	ctx := context.Background()
	s, err := Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	fsys := fstest.MapFS{
		"migrations/0001_first.sql":  {Data: []byte(`CREATE TABLE first (x int);`)},
		"migrations/0002_broken.sql": {Data: []byte(`CREATE TABLE half (x int); INSERT INTO first VALUES (1); SELECT 1/0;`)},
	}
	err = s.migrate(ctx, fsys)
	var se *Error
	if !errors.As(err, &se) || se.Code != "22012" {
		t.Fatalf("migrate = %v, want a *Error with SQLSTATE 22012 (division by zero)", err)
	}
	if v, err := s.SchemaVersion(ctx); err != nil || v != 1 {
		t.Fatalf("SchemaVersion = %d, %v; want 1", v, err)
	}
	if n, err := CountForTest(ctx, s, `SELECT count(*) FROM first`); err != nil || n != 0 {
		t.Errorf("first has %d rows (%v): 0002's insert was kept", n, err)
	}
	if n, err := CountForTest(ctx, s, `SELECT (to_regclass('half') IS NOT NULL)::int`); err != nil || n != 0 {
		t.Errorf("table half exists (%d, %v): 0002's create was kept", n, err)
	}
}
