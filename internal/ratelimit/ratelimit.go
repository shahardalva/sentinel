// Package ratelimit implements a per-client token bucket in Redis, shared by
// every gateway instance that uses the same Redis.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/shahardalva/sentinel/internal/proxy"
)

// DefaultTimeout bounds each Redis call, so a slow or unreachable Redis
// adds at most this much to a request.
const DefaultTimeout = 50 * time.Millisecond

// PoolDialTimeout bounds each attempt to set up a new pool connection: the
// DNS lookup, TCP and, for rediss://, TLS. go-redis dials on a goroutine of
// its own, never under a call's context, so this is not part of a call's
// budget: a call that has to wait for a new connection still gives up at
// its own timeout, and the dial goes on to fill the pool. 2 s covers a
// cold dial on a slow network or a retried DNS query; a dead Redis keeps a
// dial goroutine at most this long per attempt (go-redis tries 5 times).
// WarmUp's reads get it too.
const PoolDialTimeout = 2 * time.Second

// WarmConns is how many connections the gateway sets up with WarmUp before
// serving, so a burst of calls at load start finds them ready instead of
// setting them up within its timeout. At 1000 req/s and a Redis round trip
// of about 1 ms, one or two calls are in flight at once; 5 ms under load
// makes it 5. 16 leaves room for bursts (a load test starting, Poisson
// arrivals). The pool keeps them open while idle, but doesn't keep a
// minimum: go-redis's MinIdleConns would dial at construction, add as many
// again during WarmUp, and refill after every use.
const WarmConns = 16

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
// including one waiting for a new connection. New connections are dialed
// outside that budget, under PoolDialTimeout; WarmUp sets them up ahead of
// time. poolSize caps its connections; zero keeps go-redis's default of 10
// per CPU. Pass at least the number of requests expected to check their
// limit at once, or the extra ones get ErrBusy.
func NewClient(url string, timeout time.Duration, poolSize int) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	opts.ContextTimeoutEnabled = true
	opts.DialTimeout = PoolDialTimeout
	opts.ReadTimeout = timeout
	opts.WriteTimeout = timeout
	opts.PoolTimeout = timeout
	if poolSize > 0 {
		opts.PoolSize = poolSize
	}
	// go-redis retries a call that hit a dead connection on another one,
	// but sleeps 10 ms to 1 s first, under the call's context. Within 50 ms
	// that sleep only turns a fast connection error, which the breaker
	// skips, into a timeout, which it counts: after a Redis restart every
	// call on a dead pooled connection would count toward opening it.
	opts.MinRetryBackoff = -1
	opts.MaxRetryBackoff = -1
	opts.DisableIdentity = true // skip CLIENT SETINFO on connect
	// Maintenance notifications are a Redis Cloud and Enterprise feature.
	// In go-redis's default "auto" mode every new connection asks for them,
	// and on a Redis without them the first refusal switches the mode off in
	// the client's Options, which every Conn shares under a lock of its own:
	// a data race when WarmUp's connections handshake at once.
	opts.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	return redis.NewClient(opts), nil
}

// WarmUp sets up n of the client's connections, at most its pool size, and
// sends a PING on each, so they are dialed and past the handshake (HELLO,
// AUTH, SELECT) before the first request needs them; they then go back to
// the pool, idle. It runs outside any request, so its reads get
// PoolDialTimeout rather than the client's call timeout. It returns how
// many connections are ready and the first failure; once ctx is done it
// returns at once, with ctx's error, and leaves the PINGs still waiting to
// return their connections when they end. Call it once at startup, before
// serving.
func WarmUp(ctx context.Context, client *redis.Client, n int) (int, error) {
	n = max(min(n, client.Options().PoolSize), 0)
	// Shares the client's pool; never closed, since that would close it.
	slow := client.WithTimeout(PoolDialTimeout)
	// Every connection is held until all have answered or WarmUp returns,
	// so each PING gets a connection of its own.
	release := make(chan struct{})
	results := make(chan error, n)
	var returned sync.WaitGroup
	for range n {
		c := slow.Conn()
		returned.Go(func() {
			results <- c.Ping(ctx).Err()
			<-release
			c.Close()
		})
	}
	var ready int
	var first error
	for range n {
		select {
		case err := <-results:
			if err == nil {
				ready++
			} else if first == nil {
				first = err
			}
		case <-ctx.Done():
			close(release)
			return ready, ctx.Err()
		}
	}
	close(release)
	returned.Wait()
	// A PING cut off by ctx's deadline says "i/o timeout"; say why.
	if ready < n && ctx.Err() != nil {
		return ready, ctx.Err()
	}
	return ready, first
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
	// A caller that gave up says nothing about Redis's health, and neither
	// does a pooled connection found closed: go-redis drops it, and the
	// next call gets another one or a new one.
	case ctx.Err() == nil && fair && !isConnError(err):
		l.breaker.record(time.Now(), probe, err)
	case probe:
		l.breaker.release()
	}
	return res, err
}

// BreakerOpen reports whether the circuit breaker is open, so Redis is
// being skipped and the fail mode applied, for a gauge. It stays true
// while a probe is in flight, until the probe succeeds.
func (l *Limiter) BreakerOpen() bool { return l.breaker.isOpen() }

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

// isConnError reports whether err says only that an open connection was
// closed under the call, as every pooled connection is when Redis restarts
// or fails over. Over TLS, and on Windows, go-redis can't check an idle
// connection for that before using it. A dial error is not one: failing to
// connect is what an outage looks like.
func isConnError(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return false
	}
	for _, target := range []error{io.EOF, io.ErrUnexpectedEOF, net.ErrClosed, errReset, errAborted, errPipe} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
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
