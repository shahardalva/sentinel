// Package keys supplies the client keys the gateway accepts, as
// proxy.KeySource implementations. Static holds the keys from
// SENTINEL_API_KEYS; Refreshing fetches SHA-256 digests on an interval and
// swaps them in, for a key store such as a database. internal/proxy never
// imports this package; it sees only proxy.KeySource.
package keys

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shahardalva/sentinel/internal/proxy"
)

// ErrNoKeys is returned by Parse for a list with no keys in it.
var ErrNoKeys = errors.New("keys: no client keys")

// Parse splits a comma-separated key list, as SENTINEL_API_KEYS holds,
// trimming spaces around each key and dropping blank entries. A list with
// no keys left is ErrNoKeys.
func Parse(s string) ([]string, error) {
	var keys []string
	for k := range strings.SplitSeq(s, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, ErrNoKeys
	}
	return keys, nil
}

// Static is a proxy.KeySource whose keys never change.
type Static struct {
	set *proxy.KeySet
}

var _ proxy.KeySource = (*Static)(nil)

// NewStatic returns a Static accepting keys; empty keys are dropped, and
// with none left every request is rejected. Only SHA-256 digests are kept.
// rec, if not nil, is told the set's size once; Static never refreshes, so
// it reports nothing else.
func NewStatic(keys []string, rec proxy.Recorder) *Static {
	s := &Static{set: proxy.NewKeySet(proxy.DigestKeys(keys))}
	if rec != nil {
		rec.RecordKeySetSize(context.Background(), s.set.Len())
	}
	return s
}

// Keys implements proxy.KeySource. A nil Static has no keys, so it rejects
// every request rather than panicking.
func (s *Static) Keys() *proxy.KeySet {
	if s == nil {
		return nil
	}
	return s.set
}

const (
	// refreshWarnInterval spaces out the "key refresh failed" warnings, as
	// the proxy's own warnings are spaced out.
	refreshWarnInterval = 10 * time.Second
	// maxDefaultFetchTimeout caps the default FetchTimeout.
	maxDefaultFetchTimeout = 10 * time.Second
)

// Fetch returns the SHA-256 digests of every client key to accept.
//
// It must honor ctx and return soon after ctx is done: Refreshing bounds
// each fetch with ctx, and stops by cancelling it. A Fetch that ignores ctx
// blocks the refresh loop for as long as it runs: the loop records no
// failure, logs nothing and doesn't stop, and only the refresh age gauge
// (sentinel_auth_key_refresh_age_seconds) shows it. Alert on that age, not
// only on refresh errors.
//
// Its error is logged and returned from NewRefreshing as is, so its string
// must be safe to log: no password, connection string, user or database
// name. A database-backed Fetch sanitizes its driver's errors.
type Fetch func(ctx context.Context) ([][sha256.Size]byte, error)

// Config configures a Refreshing key source.
type Config struct {
	Fetch Fetch
	// Interval is the rest between the end of one refresh and the start of
	// the next. Must be positive.
	Interval time.Duration
	// FetchTimeout bounds each fetch, the first included. Zero selects
	// min(Interval/2, 10s); it must be positive and less than Interval.
	FetchTimeout time.Duration
	// Logger records failed refreshes and an empty set; nil discards.
	Logger *slog.Logger
	// Recorder, if not nil, is told the set's size and every refresh's
	// outcome.
	Recorder proxy.Recorder
	// MaxStale bounds how long the last good set is served while refreshes
	// fail. Once the last successful fetch is longer ago than this, Keys
	// returns proxy.UnavailableKeySet and every request gets a 503, so a
	// key revoked while the key store is unreachable stops working after
	// MaxStale at most, rather than never. Zero never expires the set. It
	// must be more than Interval plus FetchTimeout, the longest gap between
	// two successful refreshes when nothing fails; otherwise it would
	// expire the set between them.
	MaxStale time.Duration
	// Now is the clock for the warning throttle and MaxStale; nil selects
	// time.Now. Tests in a testing/synctest bubble pass a time offset from
	// one taken outside it (see proxy.LogThrottle).
	Now func() time.Time
}

// Refreshing is a proxy.KeySource that fetches its keys on an interval.
// Every request reads the current set with one atomic load; a refresh
// builds a new set and swaps it in, so a request in flight keeps the set it
// started with. A failed refresh keeps the last good set.
type Refreshing struct {
	cur      atomic.Pointer[proxy.KeySet]
	fetch    Fetch
	interval time.Duration
	timeout  time.Duration
	logger   *slog.Logger
	rec      proxy.Recorder
	now      func() time.Time
	warn     proxy.LogThrottle
	done     chan struct{}

	maxStale time.Duration // 0: never stale
	// base is the origin of lastOK, from now, so ages use the monotonic
	// clock.
	base time.Time
	// lastOK is the offset from base of the last successful fetch.
	lastOK atomic.Int64
	// stale is whether the set was last found stale, and staleMu orders
	// its changes, so each is logged and recorded once.
	stale   atomic.Bool
	staleMu sync.Mutex
}

var _ proxy.KeySource = (*Refreshing)(nil)

// NewRefreshing fetches the keys once and returns a Refreshing serving
// them, then refreshes them until ctx is done, resting cfg.Interval after
// each refresh. If ctx is already done, or the first fetch fails or takes
// longer than the fetch timeout, NewRefreshing returns an error and starts
// nothing: the gateway must not start without a key set it has actually
// loaded. A first fetch that succeeds with no keys is accepted, and every
// request is rejected until a refresh brings some.
func NewRefreshing(ctx context.Context, cfg Config) (*Refreshing, error) {
	if cfg.Fetch == nil {
		return nil, errors.New("keys: nil Fetch")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("keys: Interval must be positive, got %s", cfg.Interval)
	}
	if cfg.FetchTimeout == 0 {
		cfg.FetchTimeout = min(cfg.Interval/2, maxDefaultFetchTimeout)
	}
	if cfg.FetchTimeout <= 0 || cfg.FetchTimeout >= cfg.Interval {
		return nil, fmt.Errorf("keys: FetchTimeout must be positive and less than Interval (%s), got %s", cfg.Interval, cfg.FetchTimeout)
	}
	if cycle := cfg.Interval + cfg.FetchTimeout; cfg.MaxStale < 0 || (cfg.MaxStale > 0 && cfg.MaxStale <= cycle) {
		return nil, fmt.Errorf("keys: MaxStale must be 0 or more than Interval plus FetchTimeout (%s), got %s", cycle, cfg.MaxStale)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("keys: not started: %w", err)
	}
	r := &Refreshing{
		fetch:    cfg.Fetch,
		interval: cfg.Interval,
		timeout:  cfg.FetchTimeout,
		logger:   cfg.Logger,
		rec:      cfg.Recorder,
		now:      cfg.Now,
		done:     make(chan struct{}),
		maxStale: cfg.MaxStale,
		base:     cfg.Now(),
	}
	fctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	digests, err := r.fetch(fctx)
	if err != nil {
		if ctx.Err() == nil && errors.Is(fctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("keys: the key store did not answer in time (%s): %w", r.timeout, err)
		}
		return nil, fmt.Errorf("keys: initial fetch: %w", err)
	}
	r.store(ctx, digests)
	if r.maxStale > 0 && r.rec != nil {
		r.rec.RecordKeysStale(ctx, false)
	}
	go r.run(ctx)
	return r, nil
}

// Keys implements proxy.KeySource. A nil Refreshing has no keys, so it
// rejects every request rather than panicking. Past MaxStale it returns
// proxy.UnavailableKeySet.
func (r *Refreshing) Keys() *proxy.KeySet {
	if r == nil {
		return nil
	}
	if r.maxStale > 0 && r.checkStale(context.Background()) {
		return proxy.UnavailableKeySet()
	}
	return r.cur.Load()
}

// checkStale reports whether the last good set is older than maxStale, and
// logs and records a change since the last check: ERROR on going stale,
// INFO on recovering. Requests call it, and so does every refresh, so a
// change is noticed with or without traffic. Without a change it costs a
// clock read and two atomic loads.
func (r *Refreshing) checkStale(ctx context.Context) bool {
	stale := r.age() > r.maxStale
	if stale == r.stale.Load() {
		return stale
	}
	r.staleMu.Lock()
	defer r.staleMu.Unlock()
	// Again under the lock, with a fresh age: a refresh may have succeeded
	// meanwhile, and a concurrent check may have logged the change already.
	age := r.age()
	if stale = age > r.maxStale; stale == r.stale.Load() {
		return stale
	}
	r.stale.Store(stale)
	if stale {
		r.logger.LogAttrs(ctx, slog.LevelError, "client keys are stale; rejecting every request with 503",
			slog.String("last_refresh_age", age.Round(time.Millisecond).String()),
			slog.String("max_stale", r.maxStale.String()),
		)
	} else {
		r.logger.LogAttrs(ctx, slog.LevelInfo, "client keys are fresh again; accepting requests")
	}
	if r.rec != nil {
		r.rec.RecordKeysStale(ctx, stale)
	}
	return stale
}

// age is the time since the last successful fetch.
func (r *Refreshing) age() time.Duration {
	return r.now().Sub(r.base) - time.Duration(r.lastOK.Load())
}

// Done is closed once the refresh loop has stopped, after ctx is done. A
// Fetch that ignores ctx holds it open until that fetch returns.
func (r *Refreshing) Done() <-chan struct{} { return r.done }

// run refreshes until ctx is done. The timer is reset only after a refresh
// returns, so a slow or hung fetch is followed by a full interval of rest,
// never by another fetch at once.
func (r *Refreshing) run(ctx context.Context) {
	defer close(r.done)
	t := time.NewTimer(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.refresh(ctx)
			t.Reset(r.interval)
		}
	}
}

// refresh fetches the keys and swaps them in, or keeps the last good set
// and logs, throttled, if the fetch fails.
func (r *Refreshing) refresh(ctx context.Context) {
	// select picks at random when the timer and ctx are both ready, so a
	// refresh can be chosen after cancel; it must not start a fetch.
	if ctx.Err() != nil {
		return
	}
	fctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	digests, err := r.fetch(fctx)
	if err != nil {
		if ctx.Err() != nil {
			return // shutting down, not a failed refresh
		}
		if r.rec != nil {
			r.rec.RecordKeyRefresh(ctx, false)
		}
		if ok, suppressed := r.warn.Allow(r.now(), refreshWarnInterval); ok {
			r.logger.LogAttrs(ctx, slog.LevelWarn, "key refresh failed; keeping the last good set",
				slog.String("error", err.Error()),
				slog.Int("keys", r.cur.Load().Len()),
				slog.Int64("suppressed", suppressed),
			)
		}
		if r.maxStale > 0 {
			r.checkStale(ctx)
		}
		return
	}
	r.store(ctx, digests)
}

// store swaps in a set of digests and reports it.
func (r *Refreshing) store(ctx context.Context, digests [][sha256.Size]byte) {
	set := proxy.NewKeySet(digests)
	prev := r.cur.Swap(set)
	r.lastOK.Store(int64(r.now().Sub(r.base)))
	if r.maxStale > 0 {
		r.checkStale(ctx)
	}
	if set.Len() == 0 && (prev == nil || prev.Len() > 0) {
		r.logger.LogAttrs(ctx, slog.LevelWarn, "client key set is empty; every request is rejected")
	}
	if r.rec != nil {
		r.rec.RecordKeySetSize(ctx, set.Len())
		r.rec.RecordKeyRefresh(ctx, true)
	}
}
