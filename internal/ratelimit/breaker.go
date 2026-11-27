package ratelimit

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

const (
	// BreakerThreshold is how many Redis failures in a row must count before
	// the breaker opens. Calls that fail together count once (see breaker).
	BreakerThreshold = 5
	// DefaultBreakerCooldown is how long an open breaker skips Redis before
	// letting one request try it again.
	DefaultBreakerCooldown = 5 * time.Second
)

// ErrCircuitOpen is returned by Allow while the breaker is open: Redis has
// failed repeatedly and is not being called. Callers treat it like any other
// Redis failure and apply their fail mode.
var ErrCircuitOpen = errors.New("redis circuit open")

// breaker stops calling Redis after BreakerThreshold failures in a row, so a
// dead Redis costs requests nothing instead of a timeout each. After the
// cooldown one request probes Redis; success closes the breaker, failure
// keeps it open for another cooldown.
//
// It counts failures, not failed calls. A failure counts only if its call
// started after the last failure that counted ended: one stall of Redis or
// of the host fails every call in flight at once, and those are one event,
// not BreakerThreshold of them. Calls that start one after another and keep
// failing count each, so a dead Redis opens it after BreakerThreshold rounds
// of the timeout (250 ms at 50 ms), or sooner when calls fail fast.
//
// A connection error (isConnError) after a Redis restart says only that a
// pooled connection was dead: up to connSlack of them in a row don't count,
// and a probe that gets one is released. Past connSlack the endpoint is
// closing new connections too, which is an outage, and they count.
type breaker struct {
	logger   *slog.Logger
	cooldown time.Duration
	// connSlack is how many connection errors can come from dead pooled
	// connections after a restart: PoolSize/(MaxRetries+1), since each
	// failed call uses up MaxRetries+1 of them.
	connSlack int

	mu           sync.Mutex
	failures     int       // consecutive failures counted; >= BreakerThreshold means open
	lastFailure  time.Time // when the last counted failure's call ended
	connFailures int       // connection errors since the last success
	openUntil    time.Time // while open, when the next probe may run
	probing      bool      // a probe is in flight; other requests skip Redis
}

// allow reports whether Redis should be called, and whether this call is
// the probe that decides if the breaker closes.
func (b *breaker) allow(now time.Time) (call, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.failures < BreakerThreshold:
		return true, false
	case b.probing || now.Before(b.openUntil):
		return false, false
	default:
		b.probing = true
		return true, true
	}
}

// isOpen reports whether the breaker is open, probing included.
func (b *breaker) isOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failures >= BreakerThreshold
}

// release ends a probe that told nothing, because its caller gave up, so the
// next request can probe instead.
func (b *breaker) release() {
	b.mu.Lock()
	b.probing = false
	b.mu.Unlock()
}

// record notes the outcome of a Redis call allowed by allow, which started
// at start and ended at end.
func (b *breaker) record(start, end time.Time, probe bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
	}
	// While open only the probe decides. Calls already in flight when it
	// opened report on Redis as it was then: a late success must not close
	// it, or a Redis that answers some calls in time and not others would
	// flip it open and closed many times a second.
	if b.failures >= BreakerThreshold && !probe {
		return
	}
	if err == nil {
		if b.failures >= BreakerThreshold {
			b.logger.Info("redis circuit closed; calling redis again")
		}
		b.failures = 0
		b.connFailures = 0
		return
	}
	if isConnError(err) {
		b.connFailures++
		if b.connFailures <= b.connSlack {
			return // a probe is released: the next request probes
		}
	}
	// Failed together with the last counted failure: the same event. A
	// probe always starts after the breaker opened, so it always counts.
	if !probe && start.Before(b.lastFailure) {
		return
	}
	// Only a counted failure moves the line: if every failure did, under
	// steady load each call would start before the latest one ended and
	// none would count.
	b.lastFailure = end
	b.failures++
	// Open on the failure that reaches the threshold, and reopen when a
	// probe fails.
	if b.failures == BreakerThreshold || probe {
		b.openUntil = end.Add(b.cooldown)
		b.logger.Warn("redis circuit open; skipping redis and applying the fail mode",
			slog.Int("consecutive_failures", b.failures),
			slog.Int("connection_errors", b.connFailures),
			slog.Duration("retry_in", b.cooldown),
			slog.String("error", err.Error()),
		)
	}
}

// isConnError reports whether err says that a connection was closed under
// the call: what every pooled connection gives when Redis restarts or fails
// over, which go-redis can't check for before using an idle connection over
// TLS or on Windows. It is also what an endpoint that accepts and closes
// gives, so the breaker counts these only past its slack. A dial error is
// never one: failing to connect is what an outage looks like.
func isConnError(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return false
	}
	for _, target := range []error{io.EOF, io.ErrUnexpectedEOF, net.ErrClosed} {
		if errors.Is(err, target) {
			return true
		}
	}
	for _, target := range connErrnos {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
