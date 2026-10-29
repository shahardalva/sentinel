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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shahardalva/sentinel/internal/keys"
	"github.com/shahardalva/sentinel/internal/proxy"
	"github.com/shahardalva/sentinel/internal/telemetry"
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
	stale     []bool
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

func (f *fakeRecorder) RecordKeysStale(_ context.Context, stale bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stale = append(f.stale, stale)
}

func (f *fakeRecorder) staleReports() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.stale)
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

// realStart is taken when the test binary starts, outside any synctest
// bubble, so after proxy's throttle base.
var realStart = time.Now()

// bubbleClock is a Config.Now for a synctest bubble, whose clock starts in
// 2000, before the throttle's base: real time from realStart, advanced by
// the bubble's fake time. Call it inside the bubble.
func bubbleClock() func() time.Time {
	start := time.Now()
	return func() time.Time { return realStart.Add(time.Since(start)) }
}

// recorderTee reports to real Metrics and keeps a copy in a fakeRecorder.
type recorderTee struct {
	*telemetry.Metrics
	fake *fakeRecorder
}

func (r *recorderTee) RecordKeySetSize(ctx context.Context, n int) {
	r.Metrics.RecordKeySetSize(ctx, n)
	r.fake.RecordKeySetSize(ctx, n)
}

func (r *recorderTee) RecordKeyRefresh(ctx context.Context, ok bool) {
	r.Metrics.RecordKeyRefresh(ctx, ok)
	r.fake.RecordKeyRefresh(ctx, ok)
}

func (r *recorderTee) RecordKeysStale(ctx context.Context, stale bool) {
	r.Metrics.RecordKeysStale(ctx, stale)
	r.fake.RecordKeysStale(ctx, stale)
}

// refreshAge scrapes m and returns sentinel_auth_key_refresh_age_seconds.
func refreshAge(t *testing.T, m *telemetry.Metrics) float64 {
	t.Helper()
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for line := range strings.SplitSeq(w.Body.String(), "\n") {
		if strings.HasPrefix(line, "sentinel_auth_key_refresh_age_seconds") {
			v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
			if err != nil {
				t.Fatalf("%s: %v", line, err)
			}
			return v
		}
	}
	t.Fatalf("scrape has no refresh age:\n%s", w.Body.String())
	return 0
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
// every other set gets only 200 or 401, and both are seen. In a synctest
// bubble, with no real-time deadline: every requester wakes at each
// refresh instant, so on real threads its requests run alongside the swap,
// and -race checks the handoff.
func TestRefreshing_SwapDuringConcurrentRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var n atomic.Int64
		fetch := func(context.Context) ([][sha256.Size]byte, error) {
			if n.Add(1)%2 == 0 {
				return digests("always", "sometimes"), nil
			}
			return digests("always"), nil
		}
		rec := &fakeRecorder{}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: interval, Recorder: rec})
		if err != nil {
			t.Fatal(err)
		}
		h := gate(r)

		const rounds = 60 // refreshes to run through
		var wg sync.WaitGroup
		var sometimesOK, sometimesNo atomic.Int64
		for g := range 8 {
			wg.Go(func() {
				// Requester g wakes g+1 times per interval, always at the
				// refresh instant too.
				step := interval / time.Duration(g+1)
				for range rounds * (g + 1) {
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
					time.Sleep(step)
				}
			})
		}
		wg.Wait()
		cancel()
		<-r.Done()

		if sometimesOK.Load() == 0 || sometimesNo.Load() == 0 {
			t.Errorf("key in every other set: %d accepted, %d rejected; want both seen", sometimesOK.Load(), sometimesNo.Load())
		}
		// Every refresh swapped in the other set.
		sizes, _ := rec.got()
		if len(sizes) < rounds {
			t.Fatalf("%d sets stored, want at least %d", len(sizes), rounds)
		}
		for i := 1; i < len(sizes); i++ {
			if sizes[i] == sizes[i-1] {
				t.Fatalf("set sizes %v: refresh %d did not swap", sizes, i)
			}
		}
	})
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
			Now:      bubbleClock(),
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

// A refresh whose fetch hangs is cut off at FetchTimeout, not before, and
// counts as a failure that keeps the set.
func TestRefreshing_FetchTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		const timeout = 5 * time.Second
		rec := &fakeRecorder{}
		var first atomic.Bool
		fetch := func(ctx context.Context) ([][sha256.Size]byte, error) {
			if !first.Swap(true) {
				return digests("k"), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: interval, FetchTimeout: timeout, Recorder: rec, Now: bubbleClock()})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(interval + timeout - time.Nanosecond)
		synctest.Wait()
		if _, refreshes := rec.got(); !slices.Equal(refreshes, []bool{true}) {
			t.Errorf("just before the timeout: refreshes %v, want [true]", refreshes)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if _, refreshes := rec.got(); !slices.Equal(refreshes, []bool{true, false}) {
			t.Errorf("at the timeout: refreshes %v, want [true false]", refreshes)
		}
		if r.Keys().Len() != 1 {
			t.Error("a timed-out fetch changed the set")
		}
		cancel()
		<-r.Done()
	})
}

// A Fetch that ignores ctx blocks the loop: it records no failure, Done
// stays open after cancel, and only the refresh age gauge shows it, by
// growing.
func TestRefreshing_FetchIgnoringCtx(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		m, err := telemetry.New(telemetry.Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer m.Shutdown(context.Background())
		release := make(chan struct{})
		var calls atomic.Int64
		fetch := func(context.Context) ([][sha256.Size]byte, error) {
			if calls.Add(1) > 1 {
				<-release
			}
			return digests("k"), nil
		}
		rec := &recorderTee{m, &fakeRecorder{}}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: interval, Recorder: rec})
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(5 * interval) // the second fetch started at 1m and never returns
		synctest.Wait()
		if _, refreshes := rec.fake.got(); !slices.Equal(refreshes, []bool{true}) {
			t.Errorf("refreshes %v, want [true]: a blocked fetch records nothing", refreshes)
		}
		if n := calls.Load(); n != 2 {
			t.Errorf("%d fetches, want 2: the loop is blocked in the second", n)
		}
		if age := refreshAge(t, m); age != (5 * interval).Seconds() {
			t.Errorf("refresh age %v s, want %v", age, (5 * interval).Seconds())
		}
		time.Sleep(5 * interval)
		if age := refreshAge(t, m); age != (10 * interval).Seconds() {
			t.Errorf("refresh age %v s, want %v: it grows while the fetch is blocked", age, (10 * interval).Seconds())
		}

		cancel()
		synctest.Wait()
		select {
		case <-r.Done():
			t.Error("Done closed while the fetch is still blocked")
		default:
		}
		close(release)
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

// A bad Config is refused before any fetch; synctest.Test fails if a loop
// was started anyway.
func TestNewRefreshing_RejectsBadConfig(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		fetch := func(context.Context) ([][sha256.Size]byte, error) {
			calls.Add(1)
			return digests("k"), nil
		}
		for name, cfg := range map[string]keys.Config{
			"no fetch":                 {Interval: interval},
			"no interval":              {Fetch: fetch},
			"negative interval":        {Fetch: fetch, Interval: -time.Second},
			"negative timeout":         {Fetch: fetch, Interval: interval, FetchTimeout: -time.Second},
			"timeout equals interval":  {Fetch: fetch, Interval: interval, FetchTimeout: interval},
			"timeout above interval":   {Fetch: fetch, Interval: interval, FetchTimeout: 2 * interval},
			"default timeout rounds 0": {Fetch: fetch, Interval: time.Nanosecond},
			"negative max stale":       {Fetch: fetch, Interval: interval, MaxStale: -time.Second},
			// A healthy cycle is up to Interval plus FetchTimeout (10 s here)
			// apart, so a MaxStale within that would go stale between them.
			"max stale under interval": {Fetch: fetch, Interval: interval, MaxStale: interval / 2},
			"max stale one cycle":      {Fetch: fetch, Interval: interval, MaxStale: interval + 10*time.Second},
		} {
			if r, err := keys.NewRefreshing(t.Context(), cfg); r != nil || err == nil {
				t.Errorf("%s: NewRefreshing = %v, %v; want an error", name, r, err)
			}
		}
		if n := calls.Load(); n != 0 {
			t.Errorf("%d fetches for bad configs, want 0", n)
		}
	})
}

// A ctx already done fails NewRefreshing before any fetch, even one that
// would ignore ctx and succeed.
func TestNewRefreshing_CancelledCtx(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var calls atomic.Int64
		fetch := func(context.Context) ([][sha256.Size]byte, error) {
			calls.Add(1)
			return digests("k"), nil
		}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: interval})
		if r != nil || !errors.Is(err, context.Canceled) {
			t.Errorf("NewRefreshing = %v, %v; want nil and context.Canceled", r, err)
		}
		if n := calls.Load(); n != 0 {
			t.Errorf("%d fetches, want 0", n)
		}
		// synctest.Test fails here if a refresh loop was started.
	})
}

// A nil source has no keys: it rejects every request instead of panicking,
// as a *Refreshing left nil by an ignored NewRefreshing error would be.
func TestNilSourcesRejectEverything(t *testing.T) {
	for name, src := range map[string]proxy.KeySource{
		"static":     (*keys.Static)(nil),
		"refreshing": (*keys.Refreshing)(nil),
	} {
		t.Run(name, func(t *testing.T) {
			if set := src.Keys(); set != nil {
				t.Errorf("Keys() = %v, want nil", set)
			}
			h := gate(src)
			for _, k := range []string{"", "k"} {
				if got := status(h, k); got != http.StatusUnauthorized {
					t.Errorf("key %q: %d, want 401", k, got)
				}
			}
		})
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

// Review finding 1: a fetch that runs to its timeout is followed by a full
// interval of rest, never by another fetch at once. Start and end times
// are recorded, and every gap from an end to the next start is checked.
func TestRefreshing_RestsAfterTimedOutFetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var mu sync.Mutex
		var starts, ends []time.Time
		var first atomic.Bool
		fetch := func(ctx context.Context) ([][sha256.Size]byte, error) {
			if !first.Swap(true) {
				return digests("k"), nil
			}
			mu.Lock()
			starts = append(starts, time.Now())
			mu.Unlock()
			<-ctx.Done() // hangs until its timeout
			mu.Lock()
			ends = append(ends, time.Now())
			mu.Unlock()
			return nil, ctx.Err()
		}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: interval, Now: bubbleClock()})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * interval)
		synctest.Wait()
		cancel()
		<-r.Done()

		mu.Lock()
		defer mu.Unlock()
		if len(starts) < 3 {
			t.Fatalf("%d refreshes in 10 intervals, want at least 3", len(starts))
		}
		for i := 1; i < len(starts) && i-1 < len(ends); i++ {
			if gap := starts[i].Sub(ends[i-1]); gap < interval {
				t.Errorf("fetch %d started %s after fetch %d timed out, want at least %s", i+1, gap, i, interval)
			}
		}
	})
}

// Review finding 2: a first fetch that hangs fails startup once the fetch
// timeout passes, with an error saying the key store did not answer.
// Each case checks the timeout exactly: still waiting 1ns before it, done
// at it. The defaults are min(Interval/2, 10s).
func TestNewRefreshing_InitialFetchHangs(t *testing.T) {
	for _, tt := range []struct {
		name               string
		interval, explicit time.Duration
		want               time.Duration
	}{
		{"default capped at 10s", interval, 0, 10 * time.Second},
		{"default half the interval", 4 * time.Second, 0, 2 * time.Second},
		{"explicit", interval, 3 * time.Second, 3 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				hang := func(ctx context.Context) ([][sha256.Size]byte, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				done := make(chan struct{})
				var r *keys.Refreshing
				var err error
				go func() {
					defer close(done)
					r, err = keys.NewRefreshing(ctx, keys.Config{Fetch: hang, Interval: tt.interval, FetchTimeout: tt.explicit})
				}()
				time.Sleep(tt.want - time.Nanosecond)
				synctest.Wait()
				select {
				case <-done:
					t.Fatalf("NewRefreshing returned before the %s timeout: %v", tt.want, err)
				default:
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Error("NewRefreshing still waiting on a hung store after the fetch timeout")
					cancel()
					<-done
					return
				}
				if r != nil || err == nil || !strings.Contains(err.Error(), "did not answer in time") || !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("NewRefreshing = %v, %v; want an error saying the key store did not answer in time", r, err)
				}
			})
		})
	}
}

// Review finding 6: once ctx is cancelled no new fetch starts. The second
// fetch ignores ctx, outlasts an interval and returns after the cancel.
// With the ticker this left a tick pending, and select picked it about
// half the time, so the case is repeated. The timer, reset only after a
// refresh, leaves none, so this now passes without the ctx check at the
// top of refresh as well; that check covers a timer firing at the instant
// of the cancel, which can't be timed deterministically.
func TestRefreshing_NoFetchAfterCancel(t *testing.T) {
	for range 20 {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			release := make(chan struct{})
			var calls atomic.Int64
			fetch := func(context.Context) ([][sha256.Size]byte, error) {
				if calls.Add(1) == 2 {
					<-release // ignores ctx
				}
				return digests("k"), nil
			}
			r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: fetch, Interval: interval})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(3 * interval) // fetch 2 starts at 1m and is still running
			synctest.Wait()
			cancel()
			close(release)
			<-r.Done()
			if n := calls.Load(); n != 2 {
				t.Fatalf("%d fetches, want 2: a fetch started after cancel", n)
			}
		})
	}
}

// logLines returns every line in logs at level, decoded.
func logLines(t *testing.T, logs *logBuffer, level string) []map[string]any {
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
		if m["level"] == level {
			out = append(out, m)
		}
	}
	return out
}

// Refreshes failing for longer than MaxStale make every request a 503,
// valid key or not: a revoked key must not stay valid for as long as the
// key store is down. One ERROR on entering, one INFO on recovering, and
// the stale gauge follows.
func TestRefreshing_MaxStale(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var logs logBuffer
		rec := &fakeRecorder{}
		f := &fakeFetch{set: digests("k")}
		const maxStale = 3 * time.Minute
		r, err := keys.NewRefreshing(ctx, keys.Config{
			Fetch:    f.fetch,
			Interval: interval,
			MaxStale: maxStale,
			Logger:   slog.New(slog.NewJSONHandler(&logs, nil)),
			Recorder: rec,
			Now:      bubbleClock(),
		})
		if err != nil {
			t.Fatal(err)
		}
		h := gate(r)

		f.serve(nil, errors.New("store down"))
		time.Sleep(maxStale) // three failed refreshes; the last success is exactly maxStale ago
		synctest.Wait()
		if got := status(h, "k"); got != http.StatusOK {
			t.Errorf("at max stale: %d, want 200 (the last good set)", got)
		}
		time.Sleep(time.Nanosecond)
		for _, key := range []string{"k", "wrong", ""} {
			if got := status(h, key); got != http.StatusServiceUnavailable {
				t.Errorf("past max stale, key %q: %d, want 503", key, got)
			}
		}
		if !r.Keys().Unavailable() {
			t.Error("Keys() past max stale is not unavailable")
		}
		errs := logLines(t, &logs, "ERROR")
		if len(errs) != 1 || errs[0]["msg"] != "client keys are stale; rejecting every request with 503" ||
			errs[0]["max_stale"] != maxStale.String() {
			t.Fatalf("ERROR lines = %v, want one stale line with max_stale", errs)
		}
		if got := rec.staleReports(); !slices.Equal(got, []bool{false, true}) {
			t.Errorf("stale reports %v, want [false true]", got)
		}

		// Still failing: still 503, and no second ERROR.
		time.Sleep(5 * interval)
		synctest.Wait()
		if got := status(h, "k"); got != http.StatusServiceUnavailable {
			t.Errorf("still stale: %d, want 503", got)
		}
		if n := len(logLines(t, &logs, "ERROR")); n != 1 {
			t.Errorf("%d ERROR lines, want 1", n)
		}

		// A refresh that succeeds ends it.
		f.serve(digests("k2"), nil)
		time.Sleep(interval)
		synctest.Wait()
		if status(h, "k2") != http.StatusOK || status(h, "k") != http.StatusUnauthorized {
			t.Error("after recovery: want the new set served")
		}
		infos := 0
		for _, l := range logLines(t, &logs, "INFO") {
			if l["msg"] == "client keys are fresh again; accepting requests" {
				infos++
			}
		}
		if infos != 1 {
			t.Errorf("%d recovery INFO lines, want 1:\n%s", infos, logs.String())
		}
		if got := rec.staleReports(); !slices.Equal(got, []bool{false, true, false}) {
			t.Errorf("stale reports %v, want [false true false]", got)
		}

		cancel()
		<-r.Done()
	})
}

// With no request coming in, a failed refresh is what notices the set has
// gone stale, so the ERROR and the gauge don't wait for traffic.
func TestRefreshing_MaxStaleWithoutTraffic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var logs logBuffer
		rec := &fakeRecorder{}
		f := &fakeFetch{set: digests("k")}
		r, err := keys.NewRefreshing(ctx, keys.Config{
			Fetch: f.fetch, Interval: interval, MaxStale: 3 * time.Minute,
			Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Recorder: rec, Now: bubbleClock(),
		})
		if err != nil {
			t.Fatal(err)
		}
		f.serve(nil, errors.New("store down"))
		time.Sleep(4 * interval) // the 4th failure is past max stale
		synctest.Wait()
		if n := len(logLines(t, &logs, "ERROR")); n != 1 {
			t.Errorf("%d ERROR lines without traffic, want 1", n)
		}
		if got := rec.staleReports(); !slices.Equal(got, []bool{false, true}) {
			t.Errorf("stale reports %v, want [false true]", got)
		}

		// Nor does the recovery wait for a request.
		f.serve(digests("k"), nil)
		time.Sleep(interval)
		synctest.Wait()
		if n := len(logLines(t, &logs, "INFO")); n != 1 {
			t.Errorf("%d INFO lines without traffic, want the recovery's", n)
		}
		if got := rec.staleReports(); !slices.Equal(got, []bool{false, true, false}) {
			t.Errorf("stale reports %v, want [false true false]", got)
		}
		cancel()
		<-r.Done()
	})
}

// MaxStale 0 keeps the last good set however long refreshes fail.
func TestRefreshing_MaxStaleZeroNeverExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		rec := &fakeRecorder{}
		f := &fakeFetch{set: digests("k")}
		r, err := keys.NewRefreshing(ctx, keys.Config{Fetch: f.fetch, Interval: interval, Recorder: rec})
		if err != nil {
			t.Fatal(err)
		}
		f.serve(nil, errors.New("store down"))
		time.Sleep(24 * time.Hour)
		synctest.Wait()
		if got := status(gate(r), "k"); got != http.StatusOK {
			t.Errorf("after a day of failures with MaxStale 0: %d, want 200", got)
		}
		if got := rec.staleReports(); len(got) != 0 {
			t.Errorf("stale reports %v with MaxStale 0, want none", got)
		}
		cancel()
		<-r.Done()
	})
}
