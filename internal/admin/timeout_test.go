package admin_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shahardalva/sentinel/internal/admin"
	"github.com/shahardalva/sentinel/internal/store"
)

// hungStore blocks every call until its context is done, as pgx does
// against a database that never answers.
type hungStore struct{ *fakeStore }

func hang(ctx context.Context) error {
	<-ctx.Done()
	return fmt.Errorf("store: timed out: %w", ctx.Err())
}

func (hungStore) ListTenants(ctx context.Context) ([]store.Tenant, error) {
	return nil, hang(ctx)
}

func (hungStore) CreateKey(ctx context.Context, _ string) (store.Key, string, error) {
	return store.Key{}, "", hang(ctx)
}

func TestStoreTimeout(t *testing.T) {
	tests := []struct {
		name, method, path string
	}{
		{"read", http.MethodGet, "/admin/tenants"},
		{"change", http.MethodPost, "/admin/tenants/" + tenantID + "/keys"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := &syncBuffer{}
				h := admin.New(admin.Config{
					Store:  hungStore{&fakeStore{}},
					Token:  token,
					Logger: slog.New(slog.NewJSONHandler(logs, nil)),
				})
				start := time.Now()
				rec := do(h, tt.method, tt.path, "", authed())
				if took := time.Since(start); took != 5*time.Second {
					t.Errorf("answered after %v, want the 5s store timeout", took)
				}
				if rec.Code != http.StatusInternalServerError {
					t.Fatalf("status %d, want 500: %s", rec.Code, rec.Body)
				}
				if typ := errorType(t, rec); typ != "internal_error" {
					t.Errorf("type %q", typ)
				}
				errs := 0
				for _, l := range logs.lines(t) {
					if l["level"] == "ERROR" && l["msg"] == "admin store error" {
						errs++
					}
				}
				if errs != 1 {
					t.Errorf("%d ERROR lines, want 1:\n%s", errs, logs)
				}
			})
		})
	}
}
