package keys_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shahardalva/sentinel/internal/keys"
	"github.com/shahardalva/sentinel/internal/proxy"
)

const interval = time.Minute

var plainErrors = proxy.ErrorFormatterFunc(func(_ int, _ proxy.ErrorKind, msg string) (string, []byte) {
	return "text/plain; charset=utf-8", []byte(msg)
})

func digest(k string) [sha256.Size]byte { return sha256.Sum256([]byte(k)) }

func digests(ks ...string) [][sha256.Size]byte {
	var out [][sha256.Size]byte
	for _, k := range ks {
		out = append(out, digest(k))
	}
	return out
}

// fakeFetch serves whatever set or error was last given to it.
type fakeFetch struct {
	mu    sync.Mutex
	set   [][sha256.Size]byte
	err   error
	calls int
}

func (f *fakeFetch) serve(set [][sha256.Size]byte, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.set, f.err = set, err
}

func (f *fakeFetch) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeFetch) fetch(context.Context) ([][sha256.Size]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.set, f.err
}

// fakeRecorder keeps the key reports.
type fakeRecorder struct {
	mu        sync.Mutex
	sizes     []int
	refreshes []bool
}

func (f *fakeRecorder) RecordRequest(context.Context, proxy.RequestInfo) {}
func (f *fakeRecorder) RecordAuthRejection(context.Context)              {}

func (f *fakeRecorder) RecordKeySetSize(_ context.Context, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sizes = append(f.sizes, n)
}

func (f *fakeRecorder) RecordKeyRefresh(_ context.Context, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshes = append(f.refreshes, ok)
}

func (f *fakeRecorder) got() ([]int, []bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sizes), slices.Clone(f.refreshes)
}

// gate puts src in front of a handler that answers 200, as the gateway's
// routes do.
func gate(src proxy.KeySource) http.Handler {
	ok := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	return proxy.NewAuthSource(src, slog.New(slog.DiscardHandler), nil).Require(plainErrors, ok)
}

func status(h http.Handler, key string) int {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	if key != "" {
		req.Header.Set("X-Api-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code
}

// logBuffer is a log destination safe to read while the refresh loop
// writes to it.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// warnings returns every WARN line in logs, decoded.
func warnings(t *testing.T, logs *logBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatal(err)
		}
		if m["level"] == "WARN" {
			out = append(out, m)
		}
	}
	return out
}

// A key removed from the store stops working at the next refresh, and
// the keys still in it keep working.
func TestRefreshing_RemovedKeyRejectedAfterRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		f := &fakeFetch{set: digests("keep", "revoke")}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: f.fetch, Interval: interval})
		if err != nil {
			t.Fatal(err)
		}
		h := gate(r)
		if status(h, "revoke") != http.StatusOK || status(h, "keep") != http.StatusOK {
			t.Fatal("initial set not accepted")
		}

		f.serve(digests("keep"), nil)
		time.Sleep(interval - time.Nanosecond)
		synctest.Wait()
		if got := status(h, "revoke"); got != http.StatusOK {
			t.Fatalf("before the refresh: %d, want 200 (the old set)", got)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := status(h, "revoke"); got != http.StatusUnauthorized {
			t.Errorf("removed key after the refresh: %d, want 401", got)
		}
		if got := status(h, "keep"); got != http.StatusOK {
			t.Errorf("remaining key after the refresh: %d, want 200", got)
		}

		cancel()
		<-r.Done()
	})
}

// Requests racing a stream of swaps each see one whole set: a key in every
// set is never rejected, a key in none is never accepted, and a key in
// every other set gets only 200 or 401. Real time, so the swaps and the
// requests overlap; run under -race.
func TestRefreshing_SwapDuringConcurrentRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var n atomic.Int64
	fetch := func(context.Context) ([][sha256.Size]byte, error) {
		if n.Add(1)%2 == 0 {
			return digests("always", "sometimes"), nil
		}
		return digests("always"), nil
	}
	r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	h := gate(r)

	var wg sync.WaitGroup
	var sometimesOK, sometimesNo atomic.Int64
	deadline := time.Now().Add(5 * time.Second)
	for range 8 {
		wg.Go(func() {
			for n.Load() < 50 && time.Now().Before(deadline) {
				if got := status(h, "always"); got != http.StatusOK {
					t.Errorf("key in every set: %d", got)
					return
				}
				if got := status(h, "never"); got != http.StatusUnauthorized {
					t.Errorf("key in no set: %d", got)
					return
				}
				switch status(h, "sometimes") {
				case http.StatusOK:
					sometimesOK.Add(1)
				case http.StatusUnauthorized:
					sometimesNo.Add(1)
				default:
					t.Error("key in every other set: neither 200 nor 401")
					return
				}
			}
		})
	}
	wg.Wait()
	if n.Load() < 50 {
		t.Fatalf("only %d fetches in 5 s; the swaps didn't overlap the requests", n.Load())
	}
	t.Logf("%d fetches; sometimes-key %d accepted, %d rejected", n.Load(), sometimesOK.Load(), sometimesNo.Load())
	cancel()
	<-r.Done()
}

// A failed refresh keeps the last good set, is recorded, and its warning
// is throttled.
func TestRefreshing_ErrorKeepsLastSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var logs logBuffer
		rec := &fakeRecorder{}
		f := &fakeFetch{set: digests("k")}
		r, err := keys.NewRefreshing(ctx, keys.Config{
			Fetch:    f.fetch,
			Interval: time.Second, // below the 10 s warning interval
			Logger:   slog.New(slog.NewJSONHandler(&logs, nil)),
			Recorder: rec,
		})
		if err != nil {
			t.Fatal(err)
		}
		h := gate(r)

		f.serve(nil, errors.New("store down"))
		time.Sleep(3 * time.Second) // three failed refreshes
		synctest.Wait()
		if got := status(h, "k"); got != http.StatusOK {
			t.Errorf("after failed refreshes: %d, want 200 (the last good set)", got)
		}
		if r.Keys().Len() != 1 {
			t.Errorf("set size %d, want 1", r.Keys().Len())
		}
		sizes, refreshes := rec.got()
		if !slices.Equal(sizes, []int{1}) || !slices.Equal(refreshes, []bool{true, false, false, false}) {
			t.Errorf("recorded sizes %v, refreshes %v; want [1], [true false false false]", sizes, refreshes)
		}
		w := warnings(t, &logs)
		if len(w) != 1 || w[0]["msg"] != "key refresh failed; keeping the last good set" ||
			w[0]["error"] != "store down" || w[0]["keys"] != float64(1) || w[0]["suppressed"] != float64(0) {
			t.Fatalf("warnings = %v, want one refresh failure with keys 1, suppressed 0", w)
		}

		// After the warning interval the next failure logs again, with the
		// count held back.
		time.Sleep(10 * time.Second)
		synctest.Wait()
		w = warnings(t, &logs)
		if len(w) != 2 || w[1]["suppressed"] != float64(9) {
			t.Errorf("warnings = %v, want a second one with suppressed 9", w)
		}

		// Recovery swaps in the new set.
		f.serve(digests("k2"), nil)
		time.Sleep(time.Second)
		synctest.Wait()
		if status(h, "k2") != http.StatusOK || status(h, "k") != http.StatusUnauthorized {
			t.Error("recovered refresh did not swap in the new set")
		}

		cancel()
		<-r.Done()
	})
}

// An empty set rejects every request, from either source.
func TestEmptySetRejectsEverything(t *testing.T) {
	t.Run("static", func(t *testing.T) {
		h := gate(keys.NewStatic([]string{"", ""}, nil))
		for _, k := range []string{"", "k", " "} {
			if got := status(h, k); got != http.StatusUnauthorized {
				t.Errorf("key %q: %d, want 401", k, got)
			}
		}
	})
	t.Run("refreshing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var logs logBuffer
			f := &fakeFetch{set: digests("k")}
			r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: f.fetch, Interval: interval, Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
			if err != nil {
				t.Fatal(err)
			}
			h := gate(r)
			f.serve(nil, nil) // every key revoked
			time.Sleep(2 * interval)
			synctest.Wait()
			for _, k := range []string{"", "k"} {
				if got := status(h, k); got != http.StatusUnauthorized {
					t.Errorf("key %q: %d, want 401", k, got)
				}
			}
			// Warned once, on becoming empty, not on every refresh.
			w := warnings(t, &logs)
			if len(w) != 1 || w[0]["msg"] != "client key set is empty; every request is rejected" {
				t.Errorf("warnings = %v, want one empty-set warning", w)
			}
			cancel()
			<-r.Done()
		})
	})
}

// Cancelling ctx stops the loop, also mid-fetch; synctest.Test fails if
// any goroutine it started is still running at the end.
func TestRefreshing_StopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		f := &fakeFetch{set: digests("k")}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: f.fetch, Interval: interval})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		<-r.Done()
		time.Sleep(10 * interval)
		synctest.Wait()
		if n := f.count(); n != 1 {
			t.Errorf("%d fetches after cancel, want only the initial one", n)
		}
	})
	t.Run("mid-fetch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			rec := &fakeRecorder{}
			var blocked atomic.Bool
			fetch := func(ctx context.Context) ([][sha256.Size]byte, error) {
				if !blocked.Swap(true) {
					return digests("k"), nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: interval, Recorder: rec})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(interval) // the second fetch starts and blocks
			synctest.Wait()
			cancel()
			<-r.Done()
			if _, refreshes := rec.got(); !slices.Equal(refreshes, []bool{true}) {
				t.Errorf("refreshes %v, want [true]: a fetch cut off by shutdown is not a failure", refreshes)
			}
			if r.Keys().Len() != 1 {
				t.Error("shutdown changed the set")
			}
		})
	})
}

// A fetch that hangs past the interval is cut off and counts as a failure.
func TestRefreshing_FetchBoundedByInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		rec := &fakeRecorder{}
		var first atomic.Bool
		fetch := func(ctx context.Context) ([][sha256.Size]byte, error) {
			if !first.Swap(true) {
				return digests("k"), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: interval, Recorder: rec})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * interval)
		synctest.Wait()
		if _, refreshes := rec.got(); !slices.Equal(refreshes, []bool{true, false}) {
			t.Errorf("refreshes %v, want [true false]", refreshes)
		}
		if r.Keys().Len() != 1 {
			t.Error("a timed-out fetch changed the set")
		}
		cancel()
		<-r.Done()
	})
}

// The first fetch must succeed: its error is returned and no loop starts.
func TestNewRefreshing_InitialFetchFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("store down")
		rec := &fakeRecorder{}
		r, err := keys.NewRefreshing(t.Context(), keys.Config{
			Fetch:    func(context.Context) ([][sha256.Size]byte, error) { return nil, boom },
			Interval: interval,
			Recorder: rec,
		})
		if r != nil || !errors.Is(err, boom) {
			t.Fatalf("NewRefreshing = %v, %v; want nil and the fetch error", r, err)
		}
		if sizes, refreshes := rec.got(); len(sizes) != 0 || len(refreshes) != 0 {
			t.Errorf("recorded %v, %v for a source never built", sizes, refreshes)
		}
		// synctest.Test fails here if a refresh loop was started.
	})
}

func TestNewRefreshing_RejectsBadConfig(t *testing.T) {
	fetch := func(context.Context) ([][sha256.Size]byte, error) { return nil, nil }
	for _, cfg := range []keys.Config{
		{Interval: interval},
		{Fetch: fetch},
		{Fetch: fetch, Interval: -time.Second},
	} {
		if _, err := keys.NewRefreshing(t.Context(), cfg); err == nil {
			t.Errorf("NewRefreshing(%+v) succeeded", cfg)
		}
	}
}

// A successful refresh reports the new size and success.
func TestRefreshing_RecordsRefreshes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		rec := &fakeRecorder{}
		f := &fakeFetch{set: digests("a", "b")}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: f.fetch, Interval: interval, Recorder: rec})
		if err != nil {
			t.Fatal(err)
		}
		f.serve(digests("a", "b", "c"), nil)
		time.Sleep(interval)
		synctest.Wait()
		sizes, refreshes := rec.got()
		if !slices.Equal(sizes, []int{2, 3}) || !slices.Equal(refreshes, []bool{true, true}) {
			t.Errorf("sizes %v, refreshes %v; want [2 3], [true true]", sizes, refreshes)
		}
		cancel()
		<-r.Done()
	})
}

// The fetched slice is copied: changing it afterwards changes nothing.
func TestRefreshing_CopiesFetchedSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		set := digests("k")
		r, err := keys.NewRefreshing(ctx, keys.Config{
			Fetch:    func(context.Context) ([][sha256.Size]byte, error) { return set, nil },
			Interval: interval,
		})
		if err != nil {
			t.Fatal(err)
		}
		set[0] = digest("other")
		h := gate(r)
		if status(h, "k") != http.StatusOK || status(h, "other") != http.StatusUnauthorized {
			t.Error("changing the fetched slice changed the served set")
		}
		cancel()
		<-r.Done()
	})
}

func TestStatic(t *testing.T) {
	rec := &fakeRecorder{}
	s := keys.NewStatic([]string{"k1", "", "k2"}, rec)
	h := gate(s)
	for k, want := range map[string]int{"k1": 200, "k2": 200, "": 401, "k3": 401} {
		if got := status(h, k); got != want {
			t.Errorf("key %q: %d, want %d", k, got, want)
		}
	}
	// Size once, and no refresh: a Static never refreshes, so it has no
	// refresh age or errors.
	sizes, refreshes := rec.got()
	if !slices.Equal(sizes, []int{2}) || len(refreshes) != 0 {
		t.Errorf("recorded sizes %v, refreshes %v; want [2] and none", sizes, refreshes)
	}
	if s.Keys() != s.Keys() {
		t.Error("Static returned a different set")
	}
}

func TestParse(t *testing.T) {
	got, err := keys.Parse(" a ,b,, ,c ")
	if want := []string{"a", "b", "c"}; err != nil || !slices.Equal(got, want) {
		t.Errorf("Parse = %q, %v; want %q", got, err, want)
	}
	for _, s := range []string{"", " , ,"} {
		if got, err := keys.Parse(s); !errors.Is(err, keys.ErrNoKeys) || got != nil {
			t.Errorf("Parse(%q) = %q, %v; want ErrNoKeys", s, got, err)
		}
	}
}
