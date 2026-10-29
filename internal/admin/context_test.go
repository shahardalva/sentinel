package admin_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/admin"
	"github.com/shahardalva/sentinel/internal/store"
)

// blockingStore holds each call until release is closed, or until the
// call's context is done, whichever comes first, as pgx does. It reports
// the context it was given.
type blockingStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
	// fail, when set, is returned once released, as a store error that
	// isn't about the context.
	fail error
	ctxs chan context.Context
}

func newBlockingStore() *blockingStore {
	return &blockingStore{
		fakeStore: &fakeStore{},
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
		ctxs:      make(chan context.Context, 1),
	}
}

func (b *blockingStore) wait(ctx context.Context) error {
	b.ctxs <- ctx
	close(b.entered)
	select {
	case <-b.release:
	case <-ctx.Done():
		return fmt.Errorf("store: canceled: %w", ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("store: canceled: %w", err)
	}
	return b.fail
}

func (b *blockingStore) CreateKey(ctx context.Context, id string) (store.Key, string, error) {
	if err := b.wait(ctx); err != nil {
		return store.Key{}, "", err
	}
	return b.fakeStore.CreateKey(ctx, id)
}

func (b *blockingStore) RevokeKey(ctx context.Context, id string) error {
	if err := b.wait(ctx); err != nil {
		return err
	}
	return b.fakeStore.RevokeKey(ctx, id)
}

func (b *blockingStore) ListTenants(ctx context.Context) ([]store.Tenant, error) {
	if err := b.wait(ctx); err != nil {
		return nil, err
	}
	return b.fakeStore.ListTenants(ctx)
}

// serveThenDisconnect serves req and cancels its context, as a client
// going away does, once the store has the call; then it releases the
// store and waits for the handler.
func serveThenDisconnect(h http.Handler, bs *blockingStore, method, path string) (*httptest.ResponseRecorder, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(method, path, nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()
	<-bs.entered
	storeCtx := <-bs.ctxs
	cancel()
	close(bs.release)
	<-done
	return rec, storeCtx
}

func newBlockingAPI(t *testing.T, bs *blockingStore) (http.Handler, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	h := admin.New(admin.Config{
		Store:  bs,
		Token:  token,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	return h, logs
}

// checkDeadline fails unless ctx has a deadline within the store timeout.
func checkDeadline(t *testing.T, ctx context.Context) {
	t.Helper()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Error("store call has no deadline")
		return
	}
	if left := time.Until(dl); left <= 0 || left > 5*time.Second {
		t.Errorf("deadline in %v, want within 5s", left)
	}
}

func TestChange_SurvivesClientDisconnect(t *testing.T) {
	tests := []struct {
		method, path, action string
	}{
		{http.MethodPost, "/admin/tenants/" + tenantID + "/keys", "create_key"},
		{http.MethodDelete, "/admin/keys/" + keyID, "revoke_key"},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			bs := newBlockingStore()
			h, logs := newBlockingAPI(t, bs)
			_, storeCtx := serveThenDisconnect(h, bs, tt.method, tt.path)

			checkDeadline(t, storeCtx)
			if bs.callCount() != 1 {
				t.Fatalf("change did not complete: calls %v", bs.calls)
			}
			audits := 0
			for _, l := range logs.lines(t) {
				if l["msg"] == "admin" && l["action"] == tt.action {
					audits++
				}
				if l["level"] == "ERROR" {
					t.Errorf("ERROR logged: %v", l)
				}
			}
			if audits != 1 {
				t.Errorf("%d audit lines for a committed change, want 1:\n%s", audits, logs)
			}
		})
	}
}

func TestRead_HasDeadline(t *testing.T) {
	bs := newBlockingStore()
	close(bs.release)
	h, _ := newBlockingAPI(t, bs)
	rec := do(h, http.MethodGet, "/admin/tenants", "", authed())
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	checkDeadline(t, <-bs.ctxs)
}

func TestClientDisconnect(t *testing.T) {
	tests := []struct {
		name, method, path string
		fail               error
		// want499: a WARN with status 499 and no response; else an ERROR
		// and a 500, written though nobody may read it.
		want499 bool
	}{
		// A read sees the client's cancellation itself.
		{"read canceled", http.MethodGet, "/admin/tenants", nil, true},
		// A change runs on a context the client can't cancel, so an error
		// from it is the store's, whether or not the client is still there.
		{"change failed after client left", http.MethodPost, "/admin/tenants/" + tenantID + "/keys", secretErr, false},
		{"revoke failed after client left", http.MethodDelete, "/admin/keys/" + keyID, secretErr, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := newBlockingStore()
			bs.fail = tt.fail
			h, logs := newBlockingAPI(t, bs)
			rec, _ := serveThenDisconnect(h, bs, tt.method, tt.path)

			answered := rec.Body.Len() != 0
			if tt.want499 && answered {
				t.Errorf("answered a client that left: %d %s", rec.Code, rec.Body)
			}
			if !tt.want499 && (rec.Code != http.StatusInternalServerError || errorType(t, rec) != "internal_error") {
				t.Errorf("got %d %s, want a 500", rec.Code, rec.Body)
			}
			warns, errs := 0, 0
			for _, l := range logs.lines(t) {
				switch {
				case l["level"] == "ERROR":
					errs++
				case l["level"] == "WARN" && l["status"] == float64(499):
					warns++
				case l["msg"] == "admin":
					t.Errorf("audit line for a failed call: %v", l)
				}
			}
			if want := map[bool]int{true: 1, false: 0}[tt.want499]; warns != want || errs != 1-want {
				t.Errorf("%d WARN 499 and %d ERROR lines, want %d and %d:\n%s", warns, errs, want, 1-want, logs)
			}
		})
	}
}

func TestAuth_WarnPathIsCapped(t *testing.T) {
	long := "/admin/" + strings.Repeat("x", 4000) + "%FF"
	tests := []struct {
		name, path, want string
		truncated        bool
	}{
		{"short", "/admin/nope%20x", "/admin/nope%20x", false},
		{"long", long, long[:256], true},
		{"exactly 256", long[:256], long[:256], false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _, logs := newAPI(t)
			do(h, http.MethodGet, tt.path, "", nil)
			var warn map[string]any
			for _, l := range logs.lines(t) {
				if l["msg"] == "admin unauthorized" {
					warn = l
				}
			}
			if warn == nil {
				t.Fatal("no unauthorized WARN")
			}
			if warn["path"] != tt.want {
				t.Errorf("path = %.60q... (%d bytes), want %d bytes", warn["path"], len(fmt.Sprint(warn["path"])), len(tt.want))
			}
			if tt.truncated {
				if warn["path_truncated"] != true || warn["path_len"] != float64(len(tt.path)) {
					t.Errorf("path_truncated = %v, path_len = %v, want true, %d", warn["path_truncated"], warn["path_len"], len(tt.path))
				}
			} else if _, ok := warn["path_truncated"]; ok {
				t.Errorf("path_truncated set on a short path: %v", warn)
			}
		})
	}
}
