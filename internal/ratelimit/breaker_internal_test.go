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

// errConn is a connection error as isConnError sees one.
var errConn = io.EOF

func newBreaker(cooldown time.Duration) *breaker {
	return &breaker{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), cooldown: cooldown}
}

// fail records a call that started at start and failed with err 1 ms later,
// returning when it ended.
func fail(b *breaker, start time.Time, probe bool, err error) time.Time {
	end := start.Add(time.Millisecond)
	b.record(start, end, probe, err)
	return end
}

// trip opens b with BreakerThreshold failures one after another from now,
// and returns when the last one ended.
func trip(t *testing.T, b *breaker, now time.Time) time.Time {
	t.Helper()
	for range BreakerThreshold {
		if call, _ := b.allow(now); !call {
			t.Fatal("closed breaker refused a call")
		}
		now = fail(b, now, false, errRedis)
	}
	if call, _ := b.allow(now); call {
		t.Fatal("breaker did not open")
	}
	return now
}

// A call that started before the breaker opened and succeeds afterwards
// says nothing about Redis now; only the probe may close the breaker.
func TestBreaker_StaleSuccessKeepsItOpen(t *testing.T) {
	b := newBreaker(time.Second)
	start := time.Now()
	now := trip(t, b, start)

	b.record(start, now, false, nil)
	if call, _ := b.allow(now); call {
		t.Fatal("stale success closed the breaker")
	}
	b.record(start, now, false, errRedis)
	later := now.Add(time.Second)
	call, probe := b.allow(later)
	if !call || !probe {
		t.Fatalf("after cooldown: call %v, probe %v; want a probe (stale failure must not extend it)", call, probe)
	}
	b.record(later, later, true, nil)
	if call, probe := b.allow(later); !call || probe {
		t.Errorf("after a good probe: call %v, probe %v; want closed", call, probe)
	}
}

// One stall of Redis or the host fails every call in flight at once. They
// all started before the first of them failed, so they count as one
// failure: five calls caught in one 50 ms pause must not open the breaker.
func TestBreaker_OneStallCountsOnce(t *testing.T) {
	b := newBreaker(time.Second)
	t0 := time.Now()
	end := t0.Add(50 * time.Millisecond)
	for i := range 2 * BreakerThreshold {
		start := t0.Add(time.Duration(i) * time.Millisecond)
		b.record(start, end.Add(time.Duration(i)*time.Millisecond), false, errRedis)
	}
	if b.isOpen() {
		t.Fatalf("one stall opened the breaker (%d failures counted)", b.failures)
	}
	if b.failures != 1 {
		t.Errorf("failures = %d, want 1", b.failures)
	}
}

// A dead Redis under steady load: a call starts every millisecond and each
// times out 50 ms later, so calls always overlap. Only the failures that
// count may move the line a call must start after; if every failure did,
// each call would start before the latest failure and the breaker would
// never open.
func TestBreaker_OverlappingTimeoutsOpen(t *testing.T) {
	const timeout = 50 * time.Millisecond
	b := newBreaker(time.Second)
	t0 := time.Now()
	for i := range 1000 {
		start := t0.Add(time.Duration(i) * time.Millisecond)
		if call, _ := b.allow(start); !call {
			// Opened after BreakerThreshold rounds of the timeout, give or take.
			if d := start.Sub(t0); d > time.Duration(BreakerThreshold+1)*timeout {
				t.Errorf("opened after %v, want within %v", d, time.Duration(BreakerThreshold+1)*timeout)
			}
			return
		}
		b.record(start, start.Add(timeout), false, errRedis)
	}
	t.Fatal("breaker never opened under a dead Redis")
}

// After a Redis restart every pooled connection is dead and the calls that
// get one fail with a connection error. Up to connSlack of them say nothing
// about Redis; past that, new connections are being closed, which is an
// outage, and they count like any other failure.
func TestBreaker_ConnErrorsCountPastSlack(t *testing.T) {
	const slack = 4
	b := newBreaker(time.Second)
	b.connSlack = slack
	now := time.Now()
	for range slack {
		now = fail(b, now, false, errConn)
	}
	if b.failures != 0 {
		t.Fatalf("failures = %d after %d connection errors, want 0 (within the slack)", b.failures, slack)
	}
	for i := range BreakerThreshold {
		if b.isOpen() {
			t.Fatalf("open after %d connection errors past the slack, want %d", i, BreakerThreshold)
		}
		now = fail(b, now, false, errConn)
	}
	if !b.isOpen() {
		t.Error("connection errors past the slack did not open the breaker")
	}
}

// A success resets the connection error count too: the next restart gets
// the whole slack again.
func TestBreaker_SuccessResetsConnErrors(t *testing.T) {
	const slack = 4
	b := newBreaker(time.Second)
	b.connSlack = slack
	now := time.Now()
	for range slack {
		now = fail(b, now, false, errConn)
	}
	b.record(now, now, false, nil)
	for range slack {
		now = fail(b, now, false, errConn)
	}
	if b.failures != 0 {
		t.Errorf("failures = %d, want 0: the success should reset the slack", b.failures)
	}
}

// A probe that gets a connection error within the slack tells nothing and
// is released, so the next request probes; past the slack it counts, and
// the breaker stays open for another cooldown.
func TestBreaker_ProbeConnErrorRearmsPastSlack(t *testing.T) {
	const slack = 3
	const cooldown = time.Second
	b := newBreaker(cooldown)
	b.connSlack = slack
	now := trip(t, b, time.Now()).Add(cooldown)
	for i := range slack {
		call, probe := b.allow(now)
		if !call || !probe {
			t.Fatalf("probe %d: call %v, probe %v; want a probe", i+1, call, probe)
		}
		now = fail(b, now, true, errConn)
	}
	call, probe := b.allow(now)
	if !call || !probe {
		t.Fatalf("after %d released probes: call %v, probe %v; want a probe", slack, call, probe)
	}
	now = fail(b, now, true, errConn)
	if call, _ := b.allow(now); call {
		t.Error("a probe's connection error past the slack did not restart the cooldown")
	}
	if call, probe := b.allow(now.Add(cooldown)); !call || !probe {
		t.Errorf("after the new cooldown: call %v, probe %v; want a probe", call, probe)
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
				start := time.Now()
				call, probe := b.allow(start)
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
				b.record(start, time.Now(), probe, err)
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
