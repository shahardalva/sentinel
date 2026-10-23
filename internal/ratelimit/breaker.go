package ratelimit

import (
	"errors"
	"log/slog"
	"sync"
	"time"
)

const (
	// BreakerThreshold is how many Redis calls in a row must fail before the
	// breaker opens.
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
type breaker struct {
	logger   *slog.Logger
	cooldown time.Duration

	mu        sync.Mutex
	failures  int       // consecutive failures; >= BreakerThreshold means open
	openUntil time.Time // while open, when the next probe may run
	probing   bool      // a probe is in flight; other requests skip Redis
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

// release ends a probe that told nothing, because its caller gave up, so the
// next request can probe instead.
func (b *breaker) release() {
	b.mu.Lock()
	b.probing = false
	b.mu.Unlock()
}

// record notes the outcome of a Redis call allowed by allow.
func (b *breaker) record(now time.Time, probe bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
	}
	if err == nil {
		if b.failures >= BreakerThreshold {
			b.logger.Info("redis circuit closed; calling redis again")
		}
		b.failures = 0
		return
	}
	b.failures++
	// Open on the failure that reaches the threshold, and reopen when a
	// probe fails. Calls already in flight when it opened don't extend it.
	if b.failures == BreakerThreshold || probe {
		b.openUntil = now.Add(b.cooldown)
		b.logger.Warn("redis circuit open; skipping redis and applying the fail mode",
			slog.Int("consecutive_failures", b.failures),
			slog.Duration("retry_in", b.cooldown),
			slog.String("error", err.Error()),
		)
	}
}
