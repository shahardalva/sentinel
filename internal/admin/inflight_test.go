package admin_test

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shahardalva/sentinel/internal/admin"
	"github.com/shahardalva/sentinel/internal/store"
)

// poolStore models the key store's pool: store.MaxConns connections, each
// call holding one while it runs, as *store.Store's methods do. Admin
// calls hold theirs until release is closed.
type poolStore struct {
	*fakeStore
	conns   chan struct{}
	release chan struct{}
}

func newPoolStore() *poolStore {
	return &poolStore{fakeStore: &fakeStore{}, conns: make(chan struct{}, store.MaxConns), release: make(chan struct{})}
}

// hold takes a connection, waits for release, and gives it back.
func (p *poolStore) hold(ctx context.Context) error {
	select {
	case p.conns <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.conns }()
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *poolStore) ListTenants(ctx context.Context) ([]store.Tenant, error) {
	_ = p.record("ListTenants", "")
	return nil, p.hold(ctx)
}

// activeDigests is the key refresh's call on the same pool: it needs a
// connection only briefly, and gets one or not right away.
func (p *poolStore) activeDigests() ([][sha256.Size]byte, bool) {
	select {
	case p.conns <- struct{}{}:
		<-p.conns
		return nil, true
	default:
		return nil, false
	}
}

// Admin requests past MaxInFlight get a 503 at once, so a flood of them
// leaves pool connections for the key refresh, which shares the pool.
func TestInFlightCapLeavesConnectionsForRefresh(t *testing.T) {
	if admin.MaxInFlight >= store.MaxConns {
		t.Fatalf("MaxInFlight %d leaves none of the %d pool connections for the refresh", admin.MaxInFlight, store.MaxConns)
	}
	synctest.Test(t, func(t *testing.T) {
		p := newPoolStore()
		h := admin.New(admin.Config{Store: p, Token: token, Logger: slog.New(slog.DiscardHandler)})

		// More requests than the pool has connections.
		n := store.MaxConns + 2
		codes := make(chan *httptest.ResponseRecorder, n)
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() { codes <- do(h, http.MethodGet, "/admin/tenants", "", authed()) })
		}
		synctest.Wait() // every request is holding a connection or answered

		if _, ok := p.activeDigests(); !ok {
			t.Error("the key refresh got no pool connection with the admin API saturated")
		}
		// The ones turned away answered without waiting for the others.
		for range n - admin.MaxInFlight {
			rec := <-codes
			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
				t.Errorf("request over the cap: %d, Retry-After %q; want 503 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
			}
			if typ := errorType(t, rec); typ != "overloaded_error" {
				t.Errorf("type %q, want overloaded_error", typ)
			}
		}
		select {
		case rec := <-codes:
			t.Errorf("request answered %d while the cap's slots are all held, want only %d answered", rec.Code, n-admin.MaxInFlight)
		default:
		}

		close(p.release)
		wg.Wait()
		close(codes)
		for rec := range codes {
			if rec.Code != http.StatusOK {
				t.Errorf("request within the cap: %d, want 200", rec.Code)
			}
		}
		// The slots are given back.
		if rec := do(h, http.MethodGet, "/admin/tenants", "", authed()); rec.Code != http.StatusOK {
			t.Errorf("after the flood: %d, want 200", rec.Code)
		}
	})
}

// Without the token a request is a 401 even with the cap full: the token
// check comes first, and takes no slot.
func TestInFlightCapAfterToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPoolStore()
		h := admin.New(admin.Config{Store: p, Token: token, Logger: slog.New(slog.DiscardHandler)})
		var wg sync.WaitGroup
		for range admin.MaxInFlight {
			wg.Go(func() { do(h, http.MethodGet, "/admin/tenants", "", authed()) })
		}
		synctest.Wait()
		if rec := do(h, http.MethodGet, "/admin/tenants", "", nil); rec.Code != http.StatusUnauthorized {
			t.Errorf("without the token, cap full: %d, want 401", rec.Code)
		}
		close(p.release)
		wg.Wait()
	})
}

// Drain waits for the requests in flight, then turns every later one away:
// once it returns, no admin request reaches the store.
func TestDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPoolStore()
		h := admin.New(admin.Config{Store: p, Token: token, Logger: slog.New(slog.DiscardHandler)})
		inFlight := make(chan int, 1)
		go func() { inFlight <- do(h, http.MethodGet, "/admin/tenants", "", authed()).Code }()
		synctest.Wait()

		short, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := h.Drain(short); err == nil {
			t.Error("Drain = nil with a request still in flight, want its ctx's error")
		}
		// A Drain that gave up gave back the slots it took.
		if rec := do(h, http.MethodGet, "/admin/keys/"+keyID, "", authed()); rec.Code == http.StatusServiceUnavailable {
			t.Error("503 after a Drain that gave up, want its slots back")
		}

		drained := make(chan error, 1)
		go func() { drained <- h.Drain(t.Context()) }()
		synctest.Wait()
		select {
		case err := <-drained:
			t.Fatalf("Drain returned %v with a request in flight", err)
		default:
		}
		close(p.release)
		if err := <-drained; err != nil {
			t.Errorf("Drain = %v", err)
		}
		if code := <-inFlight; code != http.StatusOK {
			t.Errorf("request in flight: %d, want 200", code)
		}
		calls := p.callCount()
		if rec := do(h, http.MethodGet, "/admin/tenants", "", authed()); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("after Drain: %d, want 503", rec.Code)
		}
		if p.callCount() != calls {
			t.Error("a request after Drain reached the store")
		}
	})
}

// A Drain after one that succeeded returns nil at once: it already holds
// every slot, and waiting for a slot it holds would only time out and
// read as admin requests still running.
func TestDrainTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := admin.New(admin.Config{Store: newPoolStore(), Token: token, Logger: slog.New(slog.DiscardHandler)})
		if err := h.Drain(t.Context()); err != nil {
			t.Fatalf("first Drain = %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		if err := h.Drain(ctx); err != nil {
			t.Errorf("second Drain = %v, want nil", err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Errorf("second Drain waited %s, want none", waited)
		}
		if rec := do(h, http.MethodGet, "/admin/tenants", "", authed()); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("after two Drains: %d, want 503", rec.Code)
		}
	})
}
