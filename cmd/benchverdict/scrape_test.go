package main

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/shahardalva/sentinel/internal/proxy"
	"github.com/shahardalva/sentinel/internal/telemetry"
)

// TestParseDecisions_RealScrape reads the gateway's own /metrics, so a
// renamed metric or label breaks this test, not a bench run.
func TestParseDecisions_RealScrape(t *testing.T) {
	m, err := telemetry.New(telemetry.Config{Upstreams: []telemetry.Upstream{{Name: "anthropic", Protocol: "anthropic"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Shutdown(context.Background()) })
	ctx := context.Background()
	for outcome, n := range map[proxy.RateLimitOutcome]int{proxy.RateLimitAllowed: 3, proxy.RateLimitFailOpen: 2, proxy.RateLimitLimited: 1} {
		for range n {
			m.RecordRequest(ctx, proxy.RequestInfo{Upstream: "anthropic", Protocol: "anthropic", Status: 200, RateLimit: outcome})
		}
	}
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)

	got, err := parseDecisions(string(body))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"allowed": 3, "fail_open": 2, "limited": 1}
	if len(got) != len(want) {
		t.Fatalf("parseDecisions = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("parseDecisions[%q] = %d, want %d", k, got[k], v)
		}
	}
}
