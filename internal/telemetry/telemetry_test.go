package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/shahardalva/sentinel/internal/proxy"
)

// fakeAdmission is an Admission with fixed numbers.
type fakeAdmission struct{ inUse, limit int }

func (a fakeAdmission) InUse() int { return a.inUse }
func (a fakeAdmission) Limit() int { return a.limit }

var testUpstreams = []Upstream{{"anthropic", "anthropic"}, {"openai", "openai"}, {"mockcompat", "openai"}}

func newTestMetrics(t testing.TB, cfg Config) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	if cfg.Upstreams == nil {
		cfg.Upstreams = testUpstreams
	}
	m, err := newMetrics(cfg, reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Shutdown(context.Background()) })
	return m, reader
}

// collected is one collection: instrument name to its data, and the unit
// each instrument was created with.
type collected struct {
	data  map[string]metricdata.Aggregation
	units map[string]string
}

func collect(t *testing.T, r *sdkmetric.ManualReader) collected {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	c := collected{data: map[string]metricdata.Aggregation{}, units: map[string]string{}}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			c.data[m.Name] = m.Data
			c.units[m.Name] = m.Unit
		}
	}
	return c
}

func key(s attribute.Set) string { return s.Encoded(attribute.DefaultEncoder()) }

// sums returns a counter's values by encoded attribute set.
func (c collected) sums(t *testing.T, name string) map[string]int64 {
	t.Helper()
	s, ok := c.data[name].(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s: %T, want an int64 sum", name, c.data[name])
	}
	out := map[string]int64{}
	for _, dp := range s.DataPoints {
		out[key(dp.Attributes)] = dp.Value
	}
	return out
}

func (c collected) gauge(t *testing.T, name string) int64 {
	t.Helper()
	g, ok := c.data[name].(metricdata.Gauge[int64])
	if !ok || len(g.DataPoints) != 1 {
		t.Fatalf("%s: %#v, want one int64 gauge point", name, c.data[name])
	}
	return g.DataPoints[0].Value
}

func (c collected) hists(t *testing.T, name string) map[string]metricdata.HistogramDataPoint[float64] {
	t.Helper()
	h, ok := c.data[name].(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s: %T, want a float64 histogram", name, c.data[name])
	}
	out := map[string]metricdata.HistogramDataPoint[float64]{}
	for _, dp := range h.DataPoints {
		out[key(dp.Attributes)] = dp
	}
	return out
}

// series counts the time series in c: one per instrument and attribute set.
func (c collected) series() int {
	n := 0
	for _, d := range c.data {
		switch d := d.(type) {
		case metricdata.Sum[int64]:
			n += len(d.DataPoints)
		case metricdata.Histogram[float64]:
			n += len(d.DataPoints)
		case metricdata.Gauge[int64]:
			n += len(d.DataPoints)
		}
	}
	return n
}

const ms = time.Millisecond

func TestRecordRequest(t *testing.T) {
	breaker := false
	m, reader := newTestMetrics(t, Config{
		Admission:   fakeAdmission{inUse: 3, limit: 128},
		BreakerOpen: func() bool { return breaker },
	})
	ctx := context.Background()

	// A streamed success.
	m.RecordRequest(ctx, proxy.RequestInfo{
		Upstream: "anthropic", Protocol: "anthropic", Status: 200,
		RateLimit:         proxy.RateLimitAllowed,
		Ran:               proxy.StageRateLimit | proxy.StageReadBody | proxy.StageUpstreamTTFB | proxy.StageStream,
		RateLimitDuration: 400 * time.Microsecond, ReadBody: 2 * ms, UpstreamTTFB: 100 * ms, Stream: 50 * ms,
		Total: 153 * ms,
	})
	// A stream that broke after its 200.
	m.RecordRequest(ctx, proxy.RequestInfo{
		Upstream: "mockcompat", Protocol: "openai", Status: 200, Failed: true, Phase: proxy.PhaseStream,
		RateLimit: proxy.RateLimitFailOpen,
		Ran:       proxy.StageRateLimit | proxy.StageReadBody | proxy.StageUpstreamTTFB | proxy.StageStream,
		Total:     time.Second, UpstreamTTFB: 500 * ms, Stream: 400 * ms,
	})
	// A 429, never forwarded.
	m.RecordRequest(ctx, proxy.RequestInfo{
		Upstream: "openai", Protocol: "openai", Status: 429, Failed: true, Phase: proxy.PhaseRateLimit,
		RateLimit: proxy.RateLimitLimited, Ran: proxy.StageRateLimit,
		RateLimitDuration: ms, Total: 2 * ms,
	})
	// A client that went away waiting for the upstream.
	m.RecordRequest(ctx, proxy.RequestInfo{
		Upstream: "anthropic", Protocol: "anthropic", Status: 499, Failed: true, Phase: proxy.PhaseUpstream,
		Ran: proxy.StageReadBody | proxy.StageUpstreamTTFB, Total: 3 * time.Second, UpstreamTTFB: 3 * time.Second,
	})
	m.RecordAuthRejection(ctx)
	m.RecordAuthRejection(ctx)
	got := collect(t, reader)

	wantRequests := map[string]int64{
		"protocol=anthropic,status_class=2xx,upstream=anthropic":                1,
		"phase=stream,protocol=openai,status_class=2xx,upstream=mockcompat":     1,
		"phase=rate_limit,protocol=openai,status_class=4xx,upstream=openai":     1,
		"phase=upstream,protocol=anthropic,status_class=499,upstream=anthropic": 1,
	}
	if g := got.sums(t, "sentinel.requests"); !equal(g, wantRequests) {
		t.Errorf("sentinel.requests = %v, want %v", g, wantRequests)
	}

	dur := got.hists(t, "sentinel.request.duration")
	if dp := dur["protocol=anthropic,status_class=2xx,upstream=anthropic"]; dp.Count != 1 || !near(dp.Sum, 0.153) {
		t.Errorf("request.duration for the success: count %d, sum %v; want 1, 0.153", dp.Count, dp.Sum)
	}
	if dp := dur["protocol=anthropic,status_class=499,upstream=anthropic"]; dp.Count != 1 || !near(dp.Sum, 3) {
		t.Errorf("request.duration for the 499: count %d, sum %v; want 1, 3", dp.Count, dp.Sum)
	}
	if len(dur) != 4 {
		t.Errorf("request.duration has %d series, want 4 (phase is not one of its labels): %v", len(dur), dur)
	}

	stage := got.hists(t, "sentinel.stage.duration")
	wantStages := map[string]struct {
		count uint64
		sum   float64
	}{
		"stage=rate_limit":    {3, 0.0014},
		"stage=read_body":     {3, 0.002},
		"stage=upstream_ttfb": {3, 3.6},
		"stage=stream":        {2, 0.45},
	}
	for k, w := range wantStages {
		if dp := stage[k]; dp.Count != w.count || !near(dp.Sum, w.sum) {
			t.Errorf("stage.duration{%s}: count %d, sum %v; want %d, %v", k, dp.Count, dp.Sum, w.count, w.sum)
		}
	}

	// Only the forwarded requests: 153-2-100-50 = 1 ms, 1000-500-400 =
	// 100 ms, and 0 for the 499.
	over := got.hists(t, "sentinel.gateway.overhead")
	if dp := over["protocol=anthropic,upstream=anthropic"]; dp.Count != 2 || !near(dp.Sum, 0.001) {
		t.Errorf("overhead for anthropic: count %d, sum %v; want 2, 0.001", dp.Count, dp.Sum)
	}
	if dp := over["protocol=openai,upstream=mockcompat"]; dp.Count != 1 || !near(dp.Sum, 0.1) {
		t.Errorf("overhead for mockcompat: count %d, sum %v; want 1, 0.1", dp.Count, dp.Sum)
	}
	if _, ok := over["protocol=openai,upstream=openai"]; ok {
		t.Error("overhead recorded for a 429 that never reached the upstream")
	}

	wantDecisions := map[string]int64{"outcome=allowed": 1, "outcome=fail_open": 1, "outcome=limited": 1, "outcome=not_checked": 1}
	if g := got.sums(t, "sentinel.ratelimit.decisions"); !equal(g, wantDecisions) {
		t.Errorf("ratelimit.decisions = %v, want %v", g, wantDecisions)
	}
	if g := got.sums(t, "sentinel.auth.rejections"); !equal(g, map[string]int64{"": 2}) {
		t.Errorf("auth.rejections = %v, want 2 with no attributes", g)
	}
	if g := got.gauge(t, "sentinel.admission.in_use"); g != 3 {
		t.Errorf("admission.in_use = %d, want 3", g)
	}
	if g := got.gauge(t, "sentinel.admission.limit"); g != 128 {
		t.Errorf("admission.limit = %d, want 128", g)
	}
	if g := got.gauge(t, "sentinel.ratelimit.breaker.open"); g != 0 {
		t.Errorf("breaker.open = %d, want 0", g)
	}
	breaker = true
	if g := collect(t, reader).gauge(t, "sentinel.ratelimit.breaker.open"); g != 1 {
		t.Errorf("breaker.open with the breaker open = %d, want 1", g)
	}

	for _, name := range []string{"sentinel.request.duration", "sentinel.stage.duration", "sentinel.gateway.overhead"} {
		if u := got.units[name]; u != "s" {
			t.Errorf("%s unit = %q, want s", name, u)
		}
	}
}

func TestRecordRequest_BucketBoundaries(t *testing.T) {
	m, reader := newTestMetrics(t, Config{})
	m.RecordRequest(context.Background(), proxy.RequestInfo{
		Upstream: "anthropic", Protocol: "anthropic", Status: 200,
		Ran: proxy.StageReadBody | proxy.StageUpstreamTTFB, Total: ms, UpstreamTTFB: ms / 2,
	})
	got := collect(t, reader)
	for name, want := range map[string][]float64{
		"sentinel.request.duration": requestBuckets,
		"sentinel.stage.duration":   stageBuckets,
		"sentinel.gateway.overhead": overheadBuckets,
	} {
		for _, dp := range got.hists(t, name) {
			if len(dp.Bounds) != len(want) || dp.Bounds[0] != want[0] || dp.Bounds[len(dp.Bounds)-1] != want[len(want)-1] {
				t.Errorf("%s bounds = %v, want %v", name, dp.Bounds, want)
			}
		}
	}
	if requestBuckets[0] != 0.05 || requestBuckets[len(requestBuckets)-1] != 600 ||
		stageBuckets[0] != 0.0001 || stageBuckets[len(stageBuckets)-1] != 10 ||
		overheadBuckets[0] != 0.00005 || overheadBuckets[len(overheadBuckets)-1] != 1 {
		t.Error("bucket ranges changed: request 50 ms-600 s, stage 100 us-10 s, overhead 50 us-1 s")
	}
}

func TestRecordRequest_NoGaugesWithoutSources(t *testing.T) {
	m, reader := newTestMetrics(t, Config{})
	m.RecordAuthRejection(context.Background())
	got := collect(t, reader)
	for _, name := range []string{"sentinel.admission.in_use", "sentinel.admission.limit", "sentinel.ratelimit.breaker.open"} {
		if _, ok := got.data[name]; ok {
			t.Errorf("%s registered without a source", name)
		}
	}
}

func TestOverhead(t *testing.T) {
	tests := []struct {
		name string
		info proxy.RequestInfo
		want time.Duration
	}{
		{"all stages", proxy.RequestInfo{Total: 1000 * ms, ReadBody: 10 * ms, UpstreamTTFB: 700 * ms, Stream: 285 * ms}, 5 * ms},
		{"rate limit call counts", proxy.RequestInfo{Total: 101 * ms, RateLimitDuration: ms, UpstreamTTFB: 100 * ms}, ms},
		{"no stream", proxy.RequestInfo{Total: 250 * time.Microsecond, UpstreamTTFB: 200 * time.Microsecond}, 50 * time.Microsecond},
		{"never negative", proxy.RequestInfo{Total: ms, UpstreamTTFB: 2 * ms}, 0},
	}
	for _, tt := range tests {
		if got := Overhead(tt.info); got != tt.want {
			t.Errorf("%s: Overhead = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// Values outside the closed sets collapse to "other" rather than becoming
// new series.
func TestRecordRequest_UnknownValuesAreOther(t *testing.T) {
	m, reader := newTestMetrics(t, Config{})
	for i := range 100 {
		m.RecordRequest(context.Background(), proxy.RequestInfo{
			Upstream: "u" + string(rune('a'+i%26)) + time.Duration(i).String(), Protocol: "x",
			Status: 600 + i, Failed: true, Phase: "phase" + time.Duration(i).String(),
			RateLimit: proxy.RateLimitOutcome(50 + i%100),
		})
	}
	got := collect(t, reader)
	want := map[string]int64{"phase=other,protocol=other,status_class=other,upstream=other": 100}
	if g := got.sums(t, "sentinel.requests"); !equal(g, want) {
		t.Errorf("sentinel.requests = %v, want %v", g, want)
	}
	if g := got.sums(t, "sentinel.ratelimit.decisions"); !equal(g, map[string]int64{"outcome=other": 100}) {
		t.Errorf("ratelimit.decisions = %v, want all other", g)
	}
}

func TestStatusClass(t *testing.T) {
	for status, want := range map[int]string{200: "2xx", 204: "2xx", 400: "4xx", 401: "4xx", 413: "4xx", 429: "4xx", 498: "4xx", 499: "499", 500: "5xx", 502: "5xx", 599: "5xx", 0: "other", 101: "other", 304: "other", 600: "other"} {
		if got := statusClasses[statusClass(status)]; got != want {
			t.Errorf("statusClass(%d) = %s, want %s", status, got, want)
		}
	}
}

func BenchmarkRecordRequest(b *testing.B) {
	m, _ := newTestMetrics(b, Config{Admission: fakeAdmission{1, 128}, BreakerOpen: func() bool { return false }})
	ctx := context.Background()
	info := proxy.RequestInfo{
		Upstream: "anthropic", Protocol: "anthropic", Status: 200, RateLimit: proxy.RateLimitAllowed,
		Ran:               proxy.StageRateLimit | proxy.StageReadBody | proxy.StageUpstreamTTFB | proxy.StageStream,
		RateLimitDuration: 400 * time.Microsecond, ReadBody: ms, UpstreamTTFB: 100 * ms, Stream: 20 * ms, Total: 122 * ms,
	}
	b.ReportAllocs()
	for b.Loop() {
		m.RecordRequest(ctx, info)
	}
}

func equal(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func near(a, b float64) bool {
	d := a - b
	return d < 1e-9 && d > -1e-9
}
