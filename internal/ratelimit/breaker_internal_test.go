package ratelimit

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errRedis = errors.New("redis failed")

func newBreaker(cooldown time.Duration) *breaker {
	return &breaker{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), cooldown: cooldown}
}

// trip opens b with BreakerThreshold failures at now.
func trip(t *testing.T, b *breaker, now time.Time) {
	t.Helper()
	for range BreakerThreshold {
		if call, _ := b.allow(now); !call {
			t.Fatal("closed breaker refused a call")
		}
		b.record(now, false, errRedis)
	}
	if call, _ := b.allow(now); call {
		t.Fatal("breaker did not open")
	}
}

// A call that started before the breaker opened and succeeds afterwards
// says nothing about Redis now; only the probe may close the breaker.
func TestBreaker_StaleSuccessKeepsItOpen(t *testing.T) {
	b := newBreaker(time.Second)
	now := time.Now()
	trip(t, b, now)

	b.record(now, false, nil)
	if call, _ := b.allow(now); call {
		t.Fatal("stale success closed the breaker")
	}
	b.record(now, false, errRedis)
	call, probe := b.allow(now.Add(time.Second))
	if !call || !probe {
		t.Fatalf("after cooldown: call %v, probe %v; want a probe (stale failure must not extend it)", call, probe)
	}
	b.record(now.Add(time.Second), true, nil)
	if call, probe := b.allow(now.Add(time.Second)); !call || probe {
		t.Errorf("after a good probe: call %v, probe %v; want closed", call, probe)
	}
}

// Many requests at once, across tripping, probing and closing: at most one
// probe is ever in flight, and the breaker ends closed once Redis recovers.
func TestBreaker_ConcurrentSingleProbe(t *testing.T) {
	const cooldown = 2 * time.Millisecond
	b := newBreaker(cooldown)
	var healthy atomic.Bool
	var inFlight, maxInFlight, probes atomic.Int64

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				call, probe := b.allow(time.Now())
				if !call {
					continue
				}
				if probe {
					probes.Add(1)
					n := inFlight.Add(1)
					for m := maxInFlight.Load(); n > m && !maxInFlight.CompareAndSwap(m, n); m = maxInFlight.Load() {
					}
					time.Sleep(100 * time.Microsecond)
					inFlight.Add(-1)
				}
				var err error
				if !healthy.Load() {
					err = errRedis
				}
				b.record(time.Now(), probe, err)
			}
		})
	}
	time.Sleep(50 * time.Millisecond) // Redis down: trips, failed probes
	healthy.Store(true)
	time.Sleep(50 * time.Millisecond) // Redis back: a probe closes it
	close(stop)
	wg.Wait()

	if m := maxInFlight.Load(); m > 1 {
		t.Errorf("%d probes in flight at once, want at most 1", m)
	}
	if probes.Load() == 0 {
		t.Error("no probe ran; the test did not exercise the open state")
	}
	if call, probe := b.allow(time.Now()); !call || probe {
		t.Errorf("after recovery: call %v, probe %v; want closed", call, probe)
	}
}
