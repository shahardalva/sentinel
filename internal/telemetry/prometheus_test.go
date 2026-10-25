package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/shahardalva/sentinel/internal/proxy"
)

const dashboardFile = "../../observability/grafana/dashboards/sentinel.json"

// scrape records a request of every kind the dashboard shows, then fetches
// /metrics from Metrics built by New, over HTTP, as Prometheus would.
func scrape(t *testing.T) string {
	t.Helper()
	m, err := New(Config{Upstreams: testUpstreams, Admission: fakeAdmission{1, 128}, BreakerOpen: func() bool { return false }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Shutdown(context.Background()) })
	ctx := context.Background()
	all := proxy.StageRateLimit | proxy.StageReadBody | proxy.StageUpstreamTTFB | proxy.StageStream
	for _, info := range []proxy.RequestInfo{
		{Status: 200, RateLimit: proxy.RateLimitAllowed, Ran: all, Total: 120 * ms, UpstreamTTFB: 100 * ms},
		{Status: 502, Failed: true, Phase: proxy.PhaseUpstream, RateLimit: proxy.RateLimitFailOpen, Ran: all &^ proxy.StageStream},
		{Status: 429, Failed: true, Phase: proxy.PhaseRateLimit, RateLimit: proxy.RateLimitLimited, Ran: proxy.StageRateLimit},
		{Status: 503, Failed: true, Phase: proxy.PhaseRateLimit, RateLimit: proxy.RateLimitFailClosed, Ran: proxy.StageRateLimit},
		{Status: 503, Failed: true, Phase: proxy.PhaseAdmit, RateLimit: proxy.RateLimitAllowed, Ran: proxy.StageRateLimit | proxy.StageReadBody},
	} {
		info.Upstream, info.Protocol = "anthropic", "anthropic"
		m.RecordRequest(ctx, info)
	}
	m.RecordAuthRejection(ctx)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape: %d, %v", resp.StatusCode, err)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want the Prometheus text format", ct)
	}
	return string(body)
}

// metricNames returns the metric names in a Prometheus text exposition.
func metricNames(exposition string) map[string]bool {
	names := map[string]bool{}
	for line := range strings.SplitSeq(exposition, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.IndexAny(line, "{ "); i > 0 {
			names[line[:i]] = true
		}
	}
	return names
}

// dashboardExprs returns every "expr" in the dashboard JSON, wherever it is.
func dashboardExprs(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(dashboardFile)
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s: %v", dashboardFile, err)
	}
	var exprs []string
	var walk func(any)
	walk = func(v any) {
		switch v := v.(type) {
		case map[string]any:
			for k, c := range v {
				if s, ok := c.(string); ok && k == "expr" {
					exprs = append(exprs, s)
				}
				walk(c)
			}
		case []any:
			for _, c := range v {
				walk(c)
			}
		}
	}
	walk(doc)
	if len(exprs) == 0 {
		t.Fatalf("%s has no queries", dashboardFile)
	}
	return exprs
}

var (
	sentinelMetric = regexp.MustCompile(`\bsentinel_[a-z0-9_]+\b`)
	// label="value", but not != or =~.
	equalsMatcher = regexp.MustCompile(`([a-z_]+)="([^"]*)"`)
)

// The dashboard queries the Prometheus names (dots to underscores, a unit
// suffix, _total, _bucket), not the OpenTelemetry ones. Every name it uses,
// and every label value it matches exactly, must be in a real scrape.
func TestDashboard_QueriesExportedNames(t *testing.T) {
	exposition := scrape(t)
	names := metricNames(exposition)
	for _, want := range []string{
		"sentinel_requests_total", "sentinel_request_duration_seconds_bucket",
		"sentinel_stage_duration_seconds_bucket", "sentinel_gateway_overhead_seconds_bucket",
		"sentinel_ratelimit_decisions_total", "sentinel_auth_rejections_total",
		"sentinel_admission_in_use", "sentinel_admission_limit", "sentinel_ratelimit_breaker_open",
	} {
		if !names[want] {
			t.Errorf("scrape has no %s", want)
		}
	}

	used := 0
	for _, expr := range dashboardExprs(t) {
		for _, name := range sentinelMetric.FindAllString(expr, -1) {
			used++
			if !names[name] {
				t.Errorf("dashboard queries %s, which the exporter doesn't serve: %s", name, expr)
			}
		}
		for _, m := range equalsMatcher.FindAllStringSubmatch(expr, -1) {
			// service is added by Prometheus's scrape config.
			if m[1] == "service" || strings.Contains(expr, m[1]+`!="`+m[2]+`"`) {
				continue
			}
			if !strings.Contains(exposition, m[0]) {
				t.Errorf("dashboard matches %s, which no scraped series has: %s", m[0], expr)
			}
		}
	}
	if used == 0 {
		t.Error("dashboard uses no sentinel_ metric")
	}
}
