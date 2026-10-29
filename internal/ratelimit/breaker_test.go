package ratelimit_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/shahardalva/sentinel/internal/ratelimit"
)

// syncBuffer is a bytes.Buffer safe for the concurrent writes in these tests.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const (
	breakerTimeout  = 100 * time.Millisecond
	breakerCooldown = 300 * time.Millisecond
)

// breakerLimiter returns a limiter on url with a short timeout and cooldown,
// logging to the returned buffer.
func breakerLimiter(t *testing.T, url string) (*ratelimit.Limiter, *syncBuffer) {
	t.Helper()
	l, logs, _ := breakerLimiterClient(t, url)
	return l, logs
}

// breakerLimiterClient is breakerLimiter, also returning its Redis client.
func breakerLimiterClient(t *testing.T, url string) (*ratelimit.Limiter, *syncBuffer, *redis.Client) {
	t.Helper()
	client, err := ratelimit.NewClient(url, breakerTimeout, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	logs := new(syncBuffer)
	l, err := ratelimit.New(ratelimit.Config{
		Client:          client,
		PerMinute:       60,
		Timeout:         breakerTimeout,
		BreakerCooldown: breakerCooldown,
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return l, logs, client
}

// failN makes n calls that must fail at Redis, not at the breaker.
func failN(t *testing.T, l *ratelimit.Limiter, n int) {
	t.Helper()
	for i := range n {
		if _, err := l.Allow(context.Background(), "client"); err == nil || errors.Is(err, ratelimit.ErrCircuitOpen) {
			t.Fatalf("call %d: err = %v, want a Redis error", i+1, err)
		}
	}
}

func TestBreaker_OpensAfterConsecutiveFailures(t *testing.T) {
	l, logs := breakerLimiter(t, "redis://"+blackHole(t))
	failN(t, l, ratelimit.BreakerThreshold)

	start := time.Now()
	_, err := l.Allow(context.Background(), "client")
	if !errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatalf("after %d failures: err = %v, want ErrCircuitOpen", ratelimit.BreakerThreshold, err)
	}
	if d := time.Since(start); d > breakerTimeout/2 {
		t.Errorf("open breaker took %v; it should not wait on Redis", d)
	}
	if n := strings.Count(logs.String(), "redis circuit open"); n != 1 {
		t.Errorf("open logged %d times, want 1:\n%s", n, logs)
	}
}

func TestBreaker_FewerFailuresStayClosed(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	// Down before the client exists, so the pool's idle connections aren't
	// left over from the first server. Linux drops such dead connections
	// on Get; go-redis has no such check on Windows, where retrying the
	// call on them can use up its timeout.
	mr.Close()
	l, _, client := breakerLimiterClient(t, "redis://"+addr)
	failN(t, l, ratelimit.BreakerThreshold-1)
	if err := mr.StartAddr(addr); err != nil {
		t.Fatal(err)
	}
	// The pool's dials run in the background and outlast a call's timeout,
	// so wait for one to reach the new server instead of racing them.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis back: ping: %v", err)
	}
	// A success resets the count, so four more failures don't open it.
	if _, err := l.Allow(context.Background(), "client"); err != nil {
		t.Fatalf("Redis back: %v", err)
	}
	mr.Close()
	failN(t, l, ratelimit.BreakerThreshold-1)
}

func TestBreaker_ProbeAfterCooldownCloses(t *testing.T) {
	mr := miniredis.RunT(t)
	l, logs := breakerLimiter(t, "redis://"+mr.Addr())
	addr := mr.Addr()
	mr.Close()
	failN(t, l, ratelimit.BreakerThreshold-1)
	if l.BreakerOpen() {
		t.Fatalf("BreakerOpen after %d failures = true, want false", ratelimit.BreakerThreshold-1)
	}
	failN(t, l, 1)
	if !l.BreakerOpen() {
		t.Fatal("BreakerOpen after the threshold = false, want true")
	}
	if _, err := l.Allow(context.Background(), "client"); !errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}

	if err := mr.StartAddr(addr); err != nil {
		t.Fatal(err)
	}
	// Still open until the cooldown ends, even though Redis is back.
	if _, err := l.Allow(context.Background(), "client"); !errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatalf("before cooldown: err = %v, want ErrCircuitOpen", err)
	}
	time.Sleep(breakerCooldown)
	if res, err := l.Allow(context.Background(), "client"); err != nil || !res.Allowed {
		t.Fatalf("probe after cooldown: %+v, %v; want allowed", res, err)
	}
	if _, err := l.Allow(context.Background(), "client"); err != nil {
		t.Errorf("after a good probe: err = %v, want the breaker closed", err)
	}
	if l.BreakerOpen() {
		t.Error("BreakerOpen after a good probe = true, want false")
	}
	if !strings.Contains(logs.String(), "redis circuit closed") {
		t.Errorf("close not logged:\n%s", logs)
	}
}

func TestBreaker_FailedProbeReopens(t *testing.T) {
	l, logs := breakerLimiter(t, "redis://"+blackHole(t))
	failN(t, l, ratelimit.BreakerThreshold)
	time.Sleep(breakerCooldown)

	// One probe goes to Redis; requests arriving meanwhile skip it.
	probeDone := make(chan error, 1)
	go func() {
		_, err := l.Allow(context.Background(), "client")
		probeDone <- err
	}()
	time.Sleep(breakerTimeout / 4)
	if _, err := l.Allow(context.Background(), "client"); !errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Errorf("during probe: err = %v, want ErrCircuitOpen", err)
	}
	if err := <-probeDone; err == nil || errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatalf("probe: err = %v, want a Redis error", err)
	}

	// The failed probe starts a new cooldown.
	if _, err := l.Allow(context.Background(), "client"); !errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Errorf("after failed probe: err = %v, want ErrCircuitOpen", err)
	}
	if n := strings.Count(logs.String(), "redis circuit open"); n != 2 {
		t.Errorf("open logged %d times, want 2 (trip and failed probe):\n%s", n, logs)
	}
}

func TestBreaker_CallerCancelDoesNotCount(t *testing.T) {
	l, _ := breakerLimiter(t, "redis://"+blackHole(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range ratelimit.BreakerThreshold + 1 {
		l.Allow(ctx, "client")
	}
	// Those were the caller's doing; this one still reaches Redis.
	if _, err := l.Allow(context.Background(), "client"); errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Error("breaker opened on calls the caller cancelled")
	}
}

// A probe whose caller hangs up tells nothing about Redis; the next request
// after it must probe again rather than find the breaker stuck waiting.
func TestBreaker_AbandonedProbeReleases(t *testing.T) {
	l, _ := breakerLimiter(t, "redis://"+blackHole(t))
	failN(t, l, ratelimit.BreakerThreshold)
	time.Sleep(breakerCooldown)

	ctx, cancel := context.WithTimeout(context.Background(), breakerTimeout/10)
	defer cancel()
	if _, err := l.Allow(ctx, "client"); errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatalf("probe: err = %v, want it to reach Redis", err)
	}

	start := time.Now()
	_, err := l.Allow(context.Background(), "client")
	if errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatal("after an abandoned probe: ErrCircuitOpen; the next request should probe")
	}
	if d := time.Since(start); d < breakerTimeout {
		t.Errorf("second probe took %v; it should have waited on Redis for the %v timeout", d, breakerTimeout)
	}
}

// Requests hammering the limiter while Redis goes down and comes back: the
// breaker opens, probes and closes again, and -race sees it all.
func TestBreaker_ConcurrentOutage(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	l, logs := breakerLimiter(t, "redis://"+addr)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					l.Allow(context.Background(), "client")
				}
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	mr.Close()
	time.Sleep(2 * breakerCooldown) // trips, then at least one failed probe
	if err := mr.StartAddr(addr); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * breakerCooldown) // a probe after the cooldown closes it
	close(stop)
	wg.Wait()

	if _, err := l.Allow(context.Background(), "client"); err != nil {
		t.Errorf("after recovery: err = %v, want the breaker closed", err)
	}
	if out := logs.String(); !strings.Contains(out, "redis circuit open") || !strings.Contains(out, "redis circuit closed") {
		t.Errorf("want the breaker to open and close:\n%s", out)
	}
}
