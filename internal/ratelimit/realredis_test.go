package ratelimit_test

import (
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/ratelimit"
)

// These tests run the script on a real Redis, since miniredis runs Lua with
// gopher-lua rather than Redis's own interpreter. They are skipped unless
// SENTINEL_TEST_REDIS_URL is set; CI sets it. Each test uses its own client
// name and deletes its key, so a shared database is safe.

func realLimiter(t *testing.T, perMinute int) (*ratelimit.Limiter, string) {
	t.Helper()
	url := os.Getenv("SENTINEL_TEST_REDIS_URL")
	if url == "" {
		t.Skip("SENTINEL_TEST_REDIS_URL not set")
	}
	client, err := ratelimit.NewClient(url, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	name := "test-" + rand.Text()
	t.Cleanup(func() { client.Del(context.Background(), "sentinel:ratelimit:"+name) })
	l, err := ratelimit.New(ratelimit.Config{Client: client, PerMinute: perMinute, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return l, name
}

func TestRealRedis_BurstThenDeny(t *testing.T) {
	l, name := realLimiter(t, 5)
	for want := int64(4); want >= 0; want-- {
		if ok, remaining, _ := allow(t, l, name); !ok || remaining != want {
			t.Fatalf("Allow = %v, remaining %d; want true, %d", ok, remaining, want)
		}
	}
	ok, remaining, retry := allow(t, l, name)
	if ok || remaining != 0 {
		t.Fatalf("6th request: allowed %v, remaining %d; want denied, 0", ok, remaining)
	}
	// 12s per token, a little of which has passed since the last one.
	if retry <= 11*time.Second || retry > 12*time.Second {
		t.Errorf("RetryAfter = %v, want just under 12s", retry)
	}
}

func TestRealRedis_RefillsOnRedisClock(t *testing.T) {
	l, name := realLimiter(t, 600) // one token per 100ms
	// Drain until denied: tokens refill while the 600 calls run.
	var ok bool
	var retry time.Duration
	for range 1000 {
		if ok, _, retry = allow(t, l, name); !ok {
			break
		}
	}
	if ok || retry <= 0 || retry > 100*time.Millisecond {
		t.Fatalf("empty bucket: allowed %v, RetryAfter %v; want denied within 100ms", ok, retry)
	}
	time.Sleep(retry + 10*time.Millisecond)
	if ok, _, _ := allow(t, l, name); !ok {
		t.Error("denied after waiting RetryAfter")
	}
}

func TestRealRedis_KeyExpires(t *testing.T) {
	l, name := realLimiter(t, 60)
	allow(t, l, name)
	client, err := ratelimit.NewClient(os.Getenv("SENTINEL_TEST_REDIS_URL"), time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ttl, err := client.PTTL(context.Background(), "sentinel:ratelimit:"+name).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= time.Minute-time.Second || ttl > time.Minute+2*time.Second {
		t.Errorf("TTL = %v, want about a minute", ttl)
	}
}
