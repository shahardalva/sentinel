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
	var digests [][sha256.Size]byte
	for _, k := range keys {
		if k != "" {
			digests = append(digests, sha256.Sum256([]byte(k)))
		}
	}
	s := &Static{set: proxy.NewKeySet(digests)}
	if rec != nil {
		rec.RecordKeySetSize(context.Background(), s.set.Len())
	}
	return s
}

// Keys implements proxy.KeySource.
func (s *Static) Keys() *proxy.KeySet { return s.set }

// refreshWarnInterval spaces out the "key refresh failed" warnings, as the
// proxy's own warnings are spaced out.
const refreshWarnInterval = 10 * time.Second

// Fetch returns the SHA-256 digests of every client key to accept. Its
// error is logged, so it must not hold a secret such as a password in a
// connection string.
type Fetch func(ctx context.Context) ([][sha256.Size]byte, error)

// Config configures a Refreshing key source.
type Config struct {
	Fetch Fetch
	// Interval is the time between refreshes, and also bounds each
	// refresh's fetch. Must be positive.
	Interval time.Duration
	// Logger records failed refreshes and an empty set; nil discards.
	Logger *slog.Logger
	// Recorder, if not nil, is told the set's size and every refresh's
	// outcome.
	Recorder proxy.Recorder
}

// Refreshing is a proxy.KeySource that fetches its keys on an interval.
// Every request reads the current set with one atomic load; a refresh
// builds a new set and swaps it in, so a request in flight keeps the set it
// started with. A failed refresh keeps the last good set.
type Refreshing struct {
	cur      atomic.Pointer[proxy.KeySet]
	fetch    Fetch
	interval time.Duration
	logger   *slog.Logger
	rec      proxy.Recorder
	warn     proxy.LogThrottle
	done     chan struct{}
}

var _ proxy.KeySource = (*Refreshing)(nil)

// NewRefreshing fetches the keys once and returns a Refreshing serving
// them, refreshing every cfg.Interval until ctx is done. If the first fetch
// fails, NewRefreshing returns its error and starts nothing: the gateway
// must not start without a key set it has actually loaded.
func NewRefreshing(ctx context.Context, cfg Config) (*Refreshing, error) {
	if cfg.Fetch == nil {
		return nil, errors.New("keys: nil Fetch")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("keys: Interval must be positive, got %s", cfg.Interval)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	r := &Refreshing{
		fetch:    cfg.Fetch,
		interval: cfg.Interval,
		logger:   cfg.Logger,
		rec:      cfg.Recorder,
		done:     make(chan struct{}),
	}
	digests, err := r.fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("keys: initial fetch: %w", err)
	}
	r.store(ctx, digests)
	go r.run(ctx)
	return r, nil
}

// Keys implements proxy.KeySource.
func (r *Refreshing) Keys() *proxy.KeySet { return r.cur.Load() }

// Done is closed once the refresh loop has stopped, after ctx is done.
func (r *Refreshing) Done() <-chan struct{} { return r.done }

func (r *Refreshing) run(ctx context.Context) {
	defer close(r.done)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.refresh(ctx)
		}
	}
}

// refresh fetches the keys and swaps them in, or keeps the last good set
// and logs, throttled, if the fetch fails.
func (r *Refreshing) refresh(ctx context.Context) {
	fctx, cancel := context.WithTimeout(ctx, r.interval)
	defer cancel()
	digests, err := r.fetch(fctx)
	if err != nil {
		if ctx.Err() != nil {
			return // shutting down, not a failed refresh
		}
		if r.rec != nil {
			r.rec.RecordKeyRefresh(ctx, false)
		}
		if ok, suppressed := r.warn.Allow(time.Now(), refreshWarnInterval); ok {
			r.logger.LogAttrs(ctx, slog.LevelWarn, "key refresh failed; keeping the last good set",
				slog.String("error", err.Error()),
				slog.Int("keys", r.cur.Load().Len()),
				slog.Int64("suppressed", suppressed),
			)
		}
		return
	}
	r.store(ctx, digests)
}

// store swaps in a set of digests and reports it.
func (r *Refreshing) store(ctx context.Context, digests [][sha256.Size]byte) {
	set := proxy.NewKeySet(digests)
	prev := r.cur.Swap(set)
	if set.Len() == 0 && (prev == nil || prev.Len() > 0) {
		r.logger.LogAttrs(ctx, slog.LevelWarn, "client key set is empty; every request is rejected")
	}
	if r.rec != nil {
		r.rec.RecordKeySetSize(ctx, set.Len())
		r.rec.RecordKeyRefresh(ctx, true)
	}
}
