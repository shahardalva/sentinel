// Package telemetry implements proxy.Recorder with OpenTelemetry metrics,
// exposed in the Prometheus text format. The core (internal/proxy) never
// imports OpenTelemetry; cmd/gateway wires this package in through
// proxy.Options.Recorder.
package telemetry

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/otlptranslator"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/shahardalva/sentinel/internal/proxy"
)

const scope = "github.com/shahardalva/sentinel/internal/telemetry"

// Histogram bucket boundaries, in seconds.
var (
	// A whole request, from a fast error to a non-streaming generation at
	// the 10-minute first-byte timeout.
	requestBuckets = []float64{0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600}
	// One stage. A healthy Redis call is well under a millisecond and its
	// timeout is 50 ms; an upstream's first byte takes up to seconds.
	stageBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	// The gateway's own time: about a millisecond or less at p50 in
	// bench/RESULTS.md, so the resolution is below that.
	overheadBuckets = []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}
)

// Attribute keys. Cardinality rule: every value comes from a closed set
// fixed by code or by the gateway's configuration (the upstream names in
// SENTINEL_UPSTREAMS). Never the client key or its digest, the model, the
// path, or anything else the client chooses: each distinct value is a new
// time series, so a client-controlled label lets any client exhaust the
// monitoring system's memory, a denial of service on it. Values outside
// the closed sets are recorded as "other".
const (
	keyUpstream    = attribute.Key("upstream")     // configured upstream names
	keyProtocol    = attribute.Key("protocol")     // "anthropic" or "openai"
	keyStatusClass = attribute.Key("status_class") // statusClasses
	keyPhase       = attribute.Key("phase")        // phases, on failures only
	keyStage       = attribute.Key("stage")        // stages
	keyOutcome     = attribute.Key("outcome")      // proxy.RateLimitOutcome values
)

const other = "other"

// statusClasses are the status_class values. 499, the gateway's code for a
// client that went away, is its own class: it is neither the client's
// mistake nor a server failure.
var statusClasses = [...]string{"2xx", "4xx", "499", "5xx", other}

func statusClass(status int) int {
	switch {
	case status >= 200 && status <= 299:
		return 0
	case status == 499:
		return 2
	case status >= 400 && status <= 499:
		return 1
	case status >= 500 && status <= 599:
		return 3
	}
	return 4
}

// phases are the phase values; index 0 is a request that didn't fail,
// which has no phase attribute.
var phases = [...]string{"", proxy.PhaseRateLimit, proxy.PhaseReadBody, proxy.PhaseAdmit, proxy.PhaseUpstream, proxy.PhaseStream, other}

func phaseIndex(info proxy.RequestInfo) int {
	if !info.Failed {
		return 0
	}
	for i := 1; i < len(phases)-1; i++ {
		if phases[i] == info.Phase {
			return i
		}
	}
	return len(phases) - 1
}

// stages are the timed stages, in proxy.Stages bit order, and their stage
// values.
var stages = [...]struct {
	bit  proxy.Stages
	name string
}{
	{proxy.StageRateLimit, "rate_limit"},
	{proxy.StageReadBody, "read_body"},
	{proxy.StageUpstreamTTFB, "upstream_ttfb"},
	{proxy.StageStream, "stream"},
}

// Upstream is an upstream the gateway serves. Protocol is its Provider's
// Name(), as proxy.RequestInfo carries it.
type Upstream struct {
	Name     string
	Protocol string
}

// Config configures Metrics.
type Config struct {
	// Upstreams are the label values for upstream and protocol. A request
	// for any other pair is recorded as upstream and protocol "other".
	Upstreams []Upstream
	// Admission, if set, backs the admission gauges.
	Admission interface {
		InUse() int
		Limit() int
	}
	// BreakerOpen, if set, backs the breaker gauge; nil when rate limiting
	// is off.
	BreakerOpen func() bool
}

// Metrics records requests as OpenTelemetry metrics. It implements
// proxy.Recorder. Attribute sets are built once, in New, so recording a
// request looks them up rather than building them.
type Metrics struct {
	provider *sdkmetric.MeterProvider
	handler  http.Handler

	requests        metric.Int64Counter
	requestDuration metric.Float64Histogram
	stageDuration   metric.Float64Histogram
	overhead        metric.Float64Histogram
	decisions       metric.Int64Counter
	authRejections  metric.Int64Counter
	keyRefreshErrs  metric.Int64Counter

	// keySetSize is the last size reported plus one; zero until a key
	// source reports, so the gauge has no point before then.
	keySetSize atomic.Int64
	// lastRefresh is the offset from base of the last successful key
	// refresh plus one; zero until one, so a source that never refreshes
	// (keys.Static) has no age point.
	lastRefresh atomic.Int64
	base        time.Time        // origin of lastRefresh, on the monotonic clock
	now         func() time.Time // time.Now; tests replace it

	routes   map[route]*routeAttrs
	other    *routeAttrs
	stages   [len(stages)][]metric.RecordOption
	outcomes [len(outcomeNames)][]metric.AddOption
}

var _ proxy.Recorder = (*Metrics)(nil)

type route struct{ upstream, protocol string }

// routeAttrs are the precomputed options for one upstream.
type routeAttrs struct {
	requests [len(statusClasses)][len(phases)][]metric.AddOption
	duration [len(statusClasses)][]metric.RecordOption
	overhead []metric.RecordOption
}

// outcomeNames are the outcome values, indexed by proxy.RateLimitOutcome,
// with "other" last for a value this package doesn't know.
var outcomeNames = [...]string{
	proxy.RateLimitNotChecked.String(),
	proxy.RateLimitAllowed.String(),
	proxy.RateLimitLimited.String(),
	proxy.RateLimitFailOpen.String(),
	proxy.RateLimitFailClosed.String(),
	other,
}

// New returns Metrics exposed by Handler in the Prometheus text format,
// from a registry of its own rather than the process-wide default.
func New(cfg Config) (*Metrics, error) {
	reg := prometheus.NewRegistry()
	exp, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		// Pinned, so the names the dashboard queries don't change with the
		// exporter's default: dots become underscores, and units and _total
		// become suffixes.
		otelprom.WithTranslationStrategy(otlptranslator.UnderscoreEscapingWithSuffixes),
	)
	if err != nil {
		return nil, err
	}
	m, err := newMetrics(cfg, exp)
	if err != nil {
		return nil, err
	}
	m.handler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		// Scrapes are cheap but not free; a few at once is plenty.
		MaxRequestsInFlight: 4,
		Timeout:             10 * time.Second,
	})
	return m, nil
}

// newMetrics returns Metrics that report to reader.
func newMetrics(cfg Config, reader sdkmetric.Reader) (*Metrics, error) {
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.NewSchemaless(attribute.String("service.name", "sentinel"))),
	)
	meter := provider.Meter(scope)
	m := &Metrics{provider: provider, base: time.Now(), now: time.Now}
	var errs []error
	check := func(err error) { errs = append(errs, err) }

	var err error
	m.requests, err = meter.Int64Counter("sentinel.requests", metric.WithUnit("{request}"),
		metric.WithDescription("Requests that passed authentication, by upstream, protocol, status class and, on failure, phase."))
	check(err)
	m.requestDuration, err = meter.Float64Histogram("sentinel.request.duration", metric.WithUnit("s"),
		metric.WithDescription("Whole request duration, as the log's latency_ms."),
		metric.WithExplicitBucketBoundaries(requestBuckets...))
	check(err)
	m.stageDuration, err = meter.Float64Histogram("sentinel.stage.duration", metric.WithUnit("s"),
		metric.WithDescription("Duration of each request stage that ran."),
		metric.WithExplicitBucketBoundaries(stageBuckets...))
	check(err)
	m.overhead, err = meter.Float64Histogram("sentinel.gateway.overhead", metric.WithUnit("s"),
		metric.WithDescription("Time the gateway added to a forwarded request: total minus read body, upstream time to first byte and stream."),
		metric.WithExplicitBucketBoundaries(overheadBuckets...))
	check(err)
	m.decisions, err = meter.Int64Counter("sentinel.ratelimit.decisions", metric.WithUnit("{request}"),
		metric.WithDescription("Rate limit outcomes, one per request."))
	check(err)
	m.authRejections, err = meter.Int64Counter("sentinel.auth.rejections", metric.WithUnit("{request}"),
		metric.WithDescription("Requests rejected for a missing or unknown client key."))
	check(err)
	m.keyRefreshErrs, err = meter.Int64Counter("sentinel.auth.key_refresh.errors", metric.WithUnit("{refresh}"),
		metric.WithDescription("Failed refreshes of the client key set; each kept the last good set."))
	check(err)
	_, err = meter.Int64ObservableGauge("sentinel.auth.keys", metric.WithUnit("{key}"),
		metric.WithDescription("Client keys in the set the gateway accepts. 0 rejects every request."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if n := m.keySetSize.Load(); n > 0 {
				o.Observe(n - 1)
			}
			return nil
		}))
	check(err)
	_, err = meter.Float64ObservableGauge("sentinel.auth.key_refresh.age", metric.WithUnit("s"),
		metric.WithDescription("Time since the client key set was last fetched successfully. Only for a key source that refreshes."),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			if last := m.lastRefresh.Load(); last > 0 {
				o.Observe(max(m.now().Sub(m.base)-time.Duration(last-1), 0).Seconds())
			}
			return nil
		}))
	check(err)

	if a := cfg.Admission; a != nil {
		_, err = meter.Int64ObservableGauge("sentinel.admission.in_use", metric.WithUnit("{request}"),
			metric.WithDescription("Concurrency slots taken."),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(int64(a.InUse()))
				return nil
			}))
		check(err)
		_, err = meter.Int64ObservableGauge("sentinel.admission.limit", metric.WithUnit("{request}"),
			metric.WithDescription("Concurrency slots in all (SENTINEL_MAX_CONCURRENT)."),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(int64(a.Limit()))
				return nil
			}))
		check(err)
	}
	if open := cfg.BreakerOpen; open != nil {
		_, err = meter.Int64ObservableGauge("sentinel.ratelimit.breaker.open",
			metric.WithDescription("1 while the Redis circuit breaker is open, else 0. Only a request's probe closes it, so it stays 1 on an idle gateway after Redis recovers."),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				var v int64
				if open() {
					v = 1
				}
				o.Observe(v)
				return nil
			}))
		check(err)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	m.routes = make(map[route]*routeAttrs, len(cfg.Upstreams))
	for _, u := range cfg.Upstreams {
		m.routes[route{u.Name, u.Protocol}] = newRouteAttrs(u.Name, u.Protocol)
	}
	m.other = newRouteAttrs(other, other)
	for i, s := range stages {
		m.stages[i] = recordOpts(attribute.NewSet(keyStage.String(s.name)))
	}
	for i, o := range outcomeNames {
		m.outcomes[i] = addOpts(attribute.NewSet(keyOutcome.String(o)))
	}
	return m, nil
}

// newRouteAttrs builds the attribute sets for one upstream. The full slice
// expressions (s[:len(s):len(s)]) are load-bearing: attribute.NewSet sorts
// its argument in place, and appending to a slice with spare capacity
// would let one set's sort, or the next append, rewrite another's
// attributes. Capping the capacity makes every append copy.
func newRouteAttrs(upstream, protocol string) *routeAttrs {
	ra := &routeAttrs{}
	base := []attribute.KeyValue{keyUpstream.String(upstream), keyProtocol.String(protocol)}
	for c, class := range statusClasses {
		withClass := append(base[:len(base):len(base)], keyStatusClass.String(class))
		ra.duration[c] = recordOpts(attribute.NewSet(withClass...))
		for p, phase := range phases {
			kvs := withClass
			if phase != "" {
				kvs = append(withClass[:len(withClass):len(withClass)], keyPhase.String(phase))
			}
			ra.requests[c][p] = addOpts(attribute.NewSet(kvs...))
		}
	}
	ra.overhead = recordOpts(attribute.NewSet(base...))
	return ra
}

// Option slices are built once, so passing them on doesn't allocate.
func addOpts(s attribute.Set) []metric.AddOption {
	return []metric.AddOption{metric.WithAttributeSet(s)}
}

func recordOpts(s attribute.Set) []metric.RecordOption {
	return []metric.RecordOption{metric.WithAttributeSet(s)}
}

// Handler serves the metrics in the Prometheus text format. It is nil for
// Metrics not built by New.
func (m *Metrics) Handler() http.Handler { return m.handler }

// Shutdown stops the meter provider; recording after it does nothing.
func (m *Metrics) Shutdown(ctx context.Context) error { return m.provider.Shutdown(ctx) }

// RecordRequest implements proxy.Recorder.
func (m *Metrics) RecordRequest(ctx context.Context, info proxy.RequestInfo) {
	ra, ok := m.routes[route{info.Upstream, info.Protocol}]
	if !ok {
		ra = m.other
	}
	class := statusClass(info.Status)
	m.requests.Add(ctx, 1, ra.requests[class][phaseIndex(info)]...)
	m.requestDuration.Record(ctx, info.Total.Seconds(), ra.duration[class]...)
	durations := [len(stages)]time.Duration{info.RateLimitDuration, info.ReadBody, info.UpstreamTTFB, info.Stream}
	for i, s := range stages {
		if info.Ran.Has(s.bit) {
			m.stageDuration.Record(ctx, durations[i].Seconds(), m.stages[i]...)
		}
	}
	// Only a forwarded request has an upstream time to take away; for an
	// early rejection the "overhead" would be the whole request.
	if info.Ran.Has(proxy.StageUpstreamTTFB) {
		m.overhead.Record(ctx, Overhead(info).Seconds(), ra.overhead...)
	}
	outcome := int(info.RateLimit)
	if outcome >= len(m.outcomes)-1 {
		outcome = len(m.outcomes) - 1
	}
	m.decisions.Add(ctx, 1, m.outcomes[outcome]...)
}

// RecordAuthRejection implements proxy.Recorder.
func (m *Metrics) RecordAuthRejection(ctx context.Context) {
	m.authRejections.Add(ctx, 1)
}

// RecordKeySetSize implements proxy.Recorder.
func (m *Metrics) RecordKeySetSize(_ context.Context, n int) {
	m.keySetSize.Store(int64(max(n, 0)) + 1)
}

// RecordKeyRefresh implements proxy.Recorder.
func (m *Metrics) RecordKeyRefresh(ctx context.Context, ok bool) {
	if !ok {
		m.keyRefreshErrs.Add(ctx, 1)
		return
	}
	// Adding 0 makes the error series exist, at 0, as soon as a refreshing
	// source runs, so rate() over it works before the first failure.
	m.keyRefreshErrs.Add(ctx, 0)
	m.lastRefresh.Store(int64(m.now().Sub(m.base)) + 1)
}

// Overhead is the time the gateway itself added to a request: the total
// minus reading the client's body, waiting for the upstream's first byte,
// and relaying the stream. The rate limit call counts as overhead; it is
// the gateway's own work. Never negative.
func Overhead(info proxy.RequestInfo) time.Duration {
	return max(info.Total-info.ReadBody-info.UpstreamTTFB-info.Stream, 0)
}
