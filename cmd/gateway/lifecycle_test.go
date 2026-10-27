package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// dbConfig is a valid config for a gateway on a key database, every
// listener on a free port, with vars on top.
func dbConfig(t *testing.T, vars map[string]string) config {
	t.Helper()
	cfg, err := loadConfig(env(with(map[string]string{
		"ANTHROPIC_API_KEY":     "sk",
		"SENTINEL_ADDR":         "127.0.0.1:0",
		"SENTINEL_METRICS_ADDR": "127.0.0.1:0",
		"SENTINEL_ADMIN_ADDR":   "127.0.0.1:0",
		"SENTINEL_ADMIN_TOKEN":  testToken,
		"SENTINEL_DATABASE_URL": "postgres://fake",
	}, vars)))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// wait receives from ch, or fails the test after d.
func wait[T any](t *testing.T, ch <-chan T, d time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("timed out after %s waiting for %s", d, what)
		panic("unreachable")
	}
}

// At shutdown the admin API drains before the refresh is cancelled, and
// the pool is closed only once the refresh has stopped: pgxpool.Close
// waits for connections in use, and a store call after it fails.
func TestServe_ShutdownOrder(t *testing.T) {
	f := newFakeStore()
	// The first fetch, at startup, loads an empty set. Every later one
	// blocks until the refresh is cancelled, then takes a while to return,
	// as a real fetch unwinding its query would.
	var fetches atomic.Int32
	fetching := make(chan struct{}, 1)
	f.fetch = func(ctx context.Context) ([][sha256.Size]byte, error) {
		if fetches.Add(1) == 1 {
			return nil, nil
		}
		select {
		case fetching <- struct{}{}:
		default:
		}
		<-ctx.Done()
		if ctx.Err() != context.Canceled {
			return nil, ctx.Err() // its own fetch timeout, not shutdown
		}
		f.event("refresh cancelled")
		time.Sleep(100 * time.Millisecond)
		f.event("refresh returned")
		return nil, ctx.Err()
	}
	// An admin change held in flight until the test releases it.
	inAdmin, release := make(chan struct{}), make(chan struct{})
	f.createTenant = func(context.Context) {
		close(inAdmin)
		<-release
		f.event("admin call returned")
	}
	useStore(t, f, nil)
	// A 2 s interval gives each fetch 1 s, so the one in flight at
	// shutdown is cancelled by shutdown, well before its own timeout.
	r := startServeWith(t, dbVars(t, "postgres://fake"), func(cfg *config) { cfg.keysRefresh = 2 * time.Second })
	wait(t, fetching, 10*time.Second, "a refresh to start")

	adminCode := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, "http://"+r.admin+"/admin/tenants", strings.NewReader(`{"name":"acme"}`))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			adminCode <- 0
			return
		}
		resp.Body.Close()
		adminCode <- resp.StatusCode
	}()
	wait(t, inAdmin, 5*time.Second, "the admin call")

	r.stop()
	// Long enough for anything shutdown does without waiting on the admin
	// call to happen first.
	time.Sleep(200 * time.Millisecond)
	close(release)
	if err := wait(t, r.done, 10*time.Second, "serve to return"); err != nil {
		t.Errorf("serve = %v", err)
	}
	r.done <- nil // for the cleanup

	if code := wait(t, adminCode, 5*time.Second, "the admin response"); code != http.StatusCreated {
		t.Errorf("admin call in flight at shutdown: %d, want 201", code)
	}
	want := []string{"admin call returned", "refresh cancelled", "refresh returned", "close"}
	if got := f.eventLog(); !slices.Equal(got, want) {
		t.Errorf("shutdown order %q, want %q", got, want)
	}
}

// A signal while startup waits on the database stops the gateway cleanly:
// serve returns nil promptly, with an INFO line, not an error that would
// read as a crash.
func TestServe_SignalDuringStartup(t *testing.T) {
	for _, step := range []string{"open", "migrate", "first fetch"} {
		t.Run(step, func(t *testing.T) {
			f := newFakeStore()
			entered := make(chan struct{})
			block := func(ctx context.Context) error {
				close(entered)
				<-ctx.Done()
				return fmt.Errorf("store: %s: canceled: %w", step, ctx.Err())
			}
			switch step {
			case "open":
				useOpen(t, func(ctx context.Context, _ string) (keyStore, error) { return nil, block(ctx) })
			case "migrate":
				f.migrate = block
				useStore(t, f, nil)
			case "first fetch":
				f.fetch = func(ctx context.Context) ([][sha256.Size]byte, error) { return nil, block(ctx) }
				useStore(t, f, nil)
			}
			cfg := dbConfig(t, nil)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logs := &syncBuffer{}
			done := make(chan error, 1)
			go func() { done <- serve(ctx, slog.New(slog.NewJSONHandler(logs, nil)), cfg) }()
			wait(t, entered, 5*time.Second, "the startup step")
			cancel()
			// Every startup deadline is 10 s or more, so a prompt return
			// means the signal stopped the step.
			if err := wait(t, done, 3*time.Second, "serve to return after the signal"); err != nil {
				t.Errorf("serve = %v, want nil", err)
			}

			var interrupted bool
			for _, line := range logs.lines(t) {
				msg, _ := line["msg"].(string)
				switch {
				case msg == "startup interrupted":
					interrupted = line["level"] == "INFO"
				case line["level"] == "ERROR", strings.HasSuffix(msg, "listening"):
					t.Errorf("logged %v", line)
				}
			}
			if !interrupted {
				t.Error(`no INFO "startup interrupted" line`)
			}
			if _, closed, _ := f.state(); step != "open" && !closed {
				t.Error("store not closed")
			}
		})
	}
}

// Open and Migrate each get their own deadline. In a synctest bubble the
// clock moves only when every goroutine is blocked, so a step that waits
// on its context returns exactly at its deadline; without one it would
// block forever, which synctest reports as a deadlock.
func TestStartKeys_Deadlines(t *testing.T) {
	cfg := dbConfig(t, nil)
	block := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("store: timed out: %w", ctx.Err())
	}
	for _, tt := range []struct {
		step string
		want time.Duration
		pre  string
	}{
		{"open", storeOpenTimeout, "SENTINEL_DATABASE_URL: "},
		{"migrate", storeMigrateTimeout, "key store migration: "},
	} {
		t.Run(tt.step, func(t *testing.T) {
			f := newFakeStore()
			if tt.step == "open" {
				useOpen(t, func(ctx context.Context, _ string) (keyStore, error) { return nil, block(ctx) })
			} else {
				f.migrate = block
				useStore(t, f, nil)
			}
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				_, _, _, err := startKeys(context.Background(), slog.New(slog.DiscardHandler), cfg, nil)
				if elapsed := time.Since(start); elapsed != tt.want {
					t.Errorf("%s returned after %s, want its deadline %s", tt.step, elapsed, tt.want)
				}
				if err == nil || !strings.HasPrefix(err.Error(), tt.pre) ||
					!strings.HasSuffix(err.Error(), fmt.Sprintf("(startup deadline %s)", tt.want)) {
					t.Errorf("startKeys = %v, want a %q error naming its deadline", err, tt.pre)
				}
			})
		})
	}
}

// A fetch that ignores its context can't stop the refresh. Shutdown waits
// keysStopTimeout for it, logs that, and leaves the pool open, since
// pgxpool.Close would wait for the stuck fetch's connection.
func TestServe_StuckRefreshLeavesStoreOpen(t *testing.T) {
	defer func(d time.Duration) { keysStopTimeout = d }(keysStopTimeout)
	keysStopTimeout = 100 * time.Millisecond
	f := newFakeStore()
	var fetches atomic.Int32
	stuck, unblock := make(chan struct{}, 1), make(chan struct{})
	t.Cleanup(func() { close(unblock) }) // after serve's cleanup: registered first
	f.fetch = func(context.Context) ([][sha256.Size]byte, error) {
		if fetches.Add(1) == 1 {
			return nil, nil
		}
		select {
		case stuck <- struct{}{}:
		default:
		}
		<-unblock
		return nil, fmt.Errorf("store: active digests: late")
	}
	useStore(t, f, nil)
	r := startServeWith(t, dbVars(t, "postgres://fake"), fastRefresh)
	wait(t, stuck, 5*time.Second, "a refresh to get stuck")

	start := time.Now()
	r.stop()
	if err := wait(t, r.done, 5*time.Second, "serve to return"); err != nil {
		t.Errorf("serve = %v", err)
	}
	r.done <- nil // for the cleanup
	if elapsed := time.Since(start); elapsed < keysStopTimeout {
		t.Errorf("shutdown took %s, want at least keysStopTimeout (%s)", elapsed, keysStopTimeout)
	}
	if _, closed, _ := f.state(); closed {
		t.Error("store closed under a stuck fetch")
	}
	var warned bool
	for _, line := range r.logs.lines(t) {
		warned = warned || (line["msg"] == "key refresh did not stop; leaving the key store open" && line["level"] == "WARN")
	}
	if !warned {
		t.Error("no WARN about the refresh that did not stop")
	}
}

// An admin request still open when the admin grace period ends is cut,
// with a WARN, as the API's are.
func TestServe_AdminShutdownTimeoutWarns(t *testing.T) {
	defer func(d time.Duration) { adminShutdownTimeout = d }(adminShutdownTimeout)
	adminShutdownTimeout = 100 * time.Millisecond
	f := newFakeStore()
	inAdmin, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release) })
	f.createTenant = func(context.Context) {
		close(inAdmin)
		<-release
	}
	useStore(t, f, nil)
	r := startServe(t, dbVars(t, "postgres://fake"))
	go func() {
		req, _ := http.NewRequest(http.MethodPost, "http://"+r.admin+"/admin/tenants", strings.NewReader(`{"name":"acme"}`))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("Content-Type", "application/json")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	wait(t, inAdmin, 5*time.Second, "the admin call")
	r.stop()
	if err := wait(t, r.done, 10*time.Second, "serve to return"); err != nil {
		t.Errorf("serve = %v", err)
	}
	r.done <- nil
	var warned bool
	for _, line := range r.logs.lines(t) {
		warned = warned || (line["msg"] == "admin grace period expired; closing open admin connections" && line["level"] == "WARN")
	}
	if !warned {
		t.Error("no WARN for the admin request cut at shutdown")
	}
}
