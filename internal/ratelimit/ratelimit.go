// Package ratelimit implements a per-client token bucket in Redis, shared by
// every gateway instance that uses the same Redis.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shahardalva/sentinel/internal/proxy"
)

// DefaultTimeout bounds each Redis call, so a slow or unreachable Redis
// adds at most this much to a request.
const DefaultTimeout = 50 * time.Millisecond

const keyPrefix = "sentinel:ratelimit:"

// ErrBusy is returned by Allow when every Redis connection stayed busy for
// the whole timeout. That is load on the gateway, not a Redis failure, so it
// doesn't count toward the breaker; callers still apply their fail mode.
var ErrBusy = errors.New("ratelimit: all Redis connections busy")

// bucketScript takes one token from the bucket at KEYS[1], refilling it first
// for the time since it was last touched. It runs atomically in Redis and
// uses Redis's clock, so gateways with skewed clocks still share one bucket
// correctly.
//
// ARGV[1]: capacity in tokens. ARGV[2]: microseconds to refill one token.
// Returns {allowed (0/1), whole tokens left, microseconds until a token}.
var bucketScript = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local capacity = tonumber(ARGV[1])
local period = tonumber(ARGV[2])

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
if now > ts then
  tokens = math.min(capacity, tokens + (now - ts) / period)
end
-- Also when Redis's clock went back (failover, NTP step): that counts as no
-- time passing, instead of no refill until the clock catches up with ts.
ts = now

local allowed = 0
local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  wait = math.ceil((1 - tokens) * period)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', ts)
-- A full refill restores the initial state, so the key can go then.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * period / 1000) + 1000)
return {allowed, math.floor(tokens), wait}
`)

// Config configures a Limiter.
type Config struct {
	Client *redis.Client
	// PerMinute is the sustained rate per client, and also the burst: a full
	// bucket holds a minute's worth of tokens.
	PerMinute int
	// Timeout bounds each Redis call; zero selects DefaultTimeout.
	Timeout time.Duration
	// BreakerCooldown is how long Redis is skipped once BreakerThreshold
	// calls in a row have failed; zero selects DefaultBreakerCooldown.
	BreakerCooldown time.Duration
	// Logger records the breaker opening and closing; nil discards.
	Logger *slog.Logger
}

// Limiter is a proxy.RateLimiter backed by Redis.
type Limiter struct {
	client   *redis.Client
	capacity int
	period   float64 // microseconds per token
	timeout  time.Duration
	breaker  *breaker
	// One per connection in the client's pool, so a call holding a slot
	// never waits for a connection and its timeout is spent on Redis alone.
	slots chan struct{}
}

func New(cfg Config) (*Limiter, error) {
	if cfg.Client == nil {
		return nil, errors.New("ratelimit: nil Redis client")
	}
	if cfg.PerMinute <= 0 {
		return nil, fmt.Errorf("ratelimit: PerMinute must be positive, got %d", cfg.PerMinute)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.BreakerCooldown <= 0 {
		cfg.BreakerCooldown = DefaultBreakerCooldown
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Limiter{
		client:   cfg.Client,
		capacity: cfg.PerMinute,
		period:   float64(time.Minute/time.Microsecond) / float64(cfg.PerMinute),
		timeout:  cfg.Timeout,
		breaker:  &breaker{logger: cfg.Logger, cooldown: cfg.BreakerCooldown},
		slots:    make(chan struct{}, cfg.Client.Options().PoolSize),
	}, nil
}

// NewClient returns a Redis client for url (redis:// or rediss://) whose
// calls honor context deadlines, so Limiter's timeout bounds every call,
// including dialing. poolSize caps its connections; zero keeps go-redis's
// default of 10 per CPU. Pass at least the number of requests expected to
// check their limit at once, or the extra ones get ErrBusy.
func NewClient(url string, timeout time.Duration, poolSize int) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	opts.ContextTimeoutEnabled = true
	opts.DialTimeout = timeout
	opts.ReadTimeout = timeout
	opts.WriteTimeout = timeout
	opts.PoolTimeout = timeout
	if poolSize > 0 {
		opts.PoolSize = poolSize
	}
	opts.DisableIdentity = true // skip CLIENT SETINFO on connect
	return redis.NewClient(opts), nil
}

// Allow takes a token from client's bucket. client identifies the caller and
// becomes part of the Redis key, so it must not be a secret; the proxy passes
// a SHA-256 digest of the client's key. A non-nil error means Redis could not
// be asked, and the decision is left to the caller; while the breaker is open
// that error is ErrCircuitOpen and Redis is not called at all.
func (l *Limiter) Allow(ctx context.Context, client string) (proxy.RateLimit, error) {
	call, probe := l.breaker.allow(time.Now())
	if !call {
		return proxy.RateLimit{}, ErrCircuitOpen
	}
	callCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()

	// The wait for a slot counts against the timeout, so Allow still returns
	// within it, but says nothing about Redis.
	select {
	case l.slots <- struct{}{}:
		defer func() { <-l.slots }()
	case <-callCtx.Done():
		if probe {
			l.breaker.release()
		}
		if ctx.Err() != nil {
			return proxy.RateLimit{}, ctx.Err()
		}
		return proxy.RateLimit{}, ErrBusy
	}
	// A call left with less than half the timeout after waiting didn't get a
	// fair chance; if it times out, that's the wait's fault, not Redis's.
	d, _ := callCtx.Deadline()
	fair := time.Until(d) >= l.timeout/2

	res, err := l.run(callCtx, client)
	switch {
	case err == nil:
		l.breaker.record(time.Now(), probe, nil)
	// A caller that gave up says nothing about Redis's health.
	case ctx.Err() == nil && fair:
		l.breaker.record(time.Now(), probe, err)
	case probe:
		l.breaker.release()
	}
	return res, err
}

func (l *Limiter) run(ctx context.Context, client string) (proxy.RateLimit, error) {
	res, err := bucketScript.Run(ctx, l.client, []string{keyPrefix + client}, l.capacity, l.period).Int64Slice()
	if err != nil {
		return proxy.RateLimit{}, err
	}
	if len(res) != 3 {
		return proxy.RateLimit{}, fmt.Errorf("ratelimit: script returned %d values, want 3", len(res))
	}
	return proxy.RateLimit{
		Allowed:    res[0] == 1,
		Remaining:  res[1],
		RetryAfter: time.Duration(res[2]) * time.Microsecond,
	}, nil
}

// RouteClientLogs sends go-redis's own log output, such as a line for every
// failed dial, to logger at DEBUG. By default it goes to stderr unthrottled,
// which would flood the log during a Redis outage; the breaker and the
// proxy's fail-open warning already report outages at a bounded rate. It
// sets a process-wide logger, so call it once at startup.
func RouteClientLogs(logger *slog.Logger) {
	redis.SetLogger(clientLog{logger})
}

type clientLog struct{ logger *slog.Logger }

func (c clientLog) Printf(ctx context.Context, format string, v ...any) {
	c.logger.DebugContext(ctx, fmt.Sprintf(format, v...), slog.String("component", "go-redis"))
}
