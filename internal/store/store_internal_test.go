package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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
	for name, fsys := range map[string]fstest.MapFS{
		"gap":      {"migrations/0001_a.sql": {}, "migrations/0003_c.sql": {}},
		"bad name": {"migrations/1_a.sql": {}},
		"not one":  {"migrations/0002_b.sql": {}},
		"empty":    {"migrations/.keep": {}},
	} {
		if _, err := loadMigrations(fsys); err == nil {
			t.Errorf("%s: no error", name)
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
	} {
		if got := validName(name); got != want {
			t.Errorf("validName(%q) = %v, want %v", name, got, want)
		}
	}
}
