# ADR 0004: OpenTelemetry metrics

- Status: accepted
- Date: 2026-10-25

## Context

Until now the gateway's only telemetry was its log: one JSON line per request with per-stage timings. That answers "what happened to request X", but not "what is the p99 right now", "how close is the concurrency cap", or "is the Redis breaker open", without parsing every line. The README's headline number, the time the gateway adds to a request, was measured by hand from k6 runs (bench/RESULTS.md) and was not visible on a running gateway at all.

The project's principles say the hot path must be fast and measurable, and that a dependency needs a reason. Traces are planned next, on top of whatever this change picks.

## Decision

### OpenTelemetry, exposed for Prometheus to scrape

The gateway records metrics with the OpenTelemetry Go API and SDK (`go.opentelemetry.io/otel`, `.../metric`, `.../sdk`, `.../sdk/metric`) and exposes them with the OpenTelemetry Prometheus exporter (`go.opentelemetry.io/otel/exporters/prometheus`) on `GET /metrics`. The compose stack adds Prometheus to scrape it and Grafana with a provisioned dashboard.

The case for OpenTelemetry, on its own terms:

- **Vendor-neutral API.** Instruments are created and recorded through the OpenTelemetry API. What happens to the data is a choice of exporter at startup: Prometheus pull today; OTLP push to a collector, Grafana Cloud, Datadog, Honeycomb or anything else later, without touching a single recording call.
- **One SDK for traces next.** The trace API and SDK are part of the same project, released together, and share the resource, context propagation and exemplars. Traces will add a tracer provider beside the meter provider, not a second telemetry stack. `trace` is already in the module graph as a dependency of the SDK.
- **Swap the exporter, not the instrumentation.** Moving from scraping to OTLP is a change in `internal/telemetry` only; neither the proxy core nor the dashboard's meaning changes.

Alternatives considered:

- **`prometheus/client_golang` alone.** Mature, and it would be fewer modules today. But it is tied to one backend's model and pull protocol; adding traces would mean OpenTelemetry anyway, and then two metrics APIs, or a rewrite of every instrument.
- **Hand-written exposition.** The text format is simple, and a counter is a few lines. Histograms with fixed buckets, label escaping, concurrent updates without a lock per request, and content negotiation are not, and none of it would carry over to traces or OTLP. For a security component, a hand-rolled format parser on an exposed port is also code to review that buys nothing.

### The dependency cost

This is a large step against "stdlib first". Until now the gateway linked four third-party modules (`go-redis` and three it needs). This change adds seven direct requirements and twelve indirect ones in `go.mod`. On Linux, 19 more modules are linked into the gateway, and the stripped binary grows from 9.1 MiB to 12.4 MiB.

Added to `go.mod`, direct:

- `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/metric`, `go.opentelemetry.io/otel/sdk`, `go.opentelemetry.io/otel/sdk/metric` (v1.47.0, stable)
- `go.opentelemetry.io/otel/exporters/prometheus` (v0.69.0; the exporter has no 1.0 release yet)
- `github.com/prometheus/client_golang` (v1.25.0): the registry and the `promhttp` handler
- `github.com/prometheus/otlptranslator` (v1.0.0): to pin the name translation strategy

Indirect: `go.opentelemetry.io/otel/trace`, `go.opentelemetry.io/otel/log`, `go.opentelemetry.io/auto/sdk`, `github.com/go-logr/logr`, `github.com/go-logr/stdr`, `github.com/google/uuid`, `github.com/prometheus/client_model`, `github.com/prometheus/common`, `github.com/prometheus/procfs`, `github.com/beorn7/perks`, `github.com/munnerz/goautoneg`, and `google.golang.org/protobuf`. `github.com/cespare/xxhash/v2`, already there for go-redis, is now shared with client_golang.

**The OpenTelemetry Prometheus exporter is built on `prometheus/client_golang`.** It registers a collector in a client_golang registry, and serving it takes client_golang's `promhttp`, so client_golang is a direct dependency, not something this choice avoids. client_golang in turn brings `client_model`, `common` and protobuf. OpenTelemetry is chosen for the reasons above, not to avoid client_golang. When the exporter becomes OTLP, client_golang and its tree can leave.

All of these are maintained by the OpenTelemetry and Prometheus projects (CNCF) or are their long-standing dependencies. None of it is on the request path except the SDK's in-memory aggregation; the exporter and the HTTP handler run only when Prometheus scrapes.

### Dependency inversion: the Recorder seam

`internal/proxy` defines a `Recorder` interface and a `RequestInfo` struct and never imports OpenTelemetry. The Handler calls `RecordRequest` once per request, from the same deferred block that writes the log line, with values it already had: upstream, protocol, status, the failure phase, the rate-limit outcome, which stages ran, and their durations. `Auth` (behind `RequireKey`) calls `RecordAuthRejection` once per rejected key. `Options.Recorder` nil means no metrics and no cost beyond a nil check.

`internal/telemetry` implements the interface with OpenTelemetry, and `cmd/gateway` wires it in. A test fake implements it in the proxy's tests. The core's tests, its log line and its wire behavior don't change with the metrics backend.

`RequestInfo` says which stages ran (`Ran`), rather than leaving it to durations above zero: Windows' monotonic clock ticks coarsely enough that a fast stage can measure 0.

### A separate listener, on loopback

Metrics are served on their own listener, `SENTINEL_METRICS_ADDR`, default `127.0.0.1:9091`, with only `GET /metrics`. They are never on the API port:

- The API port faces clients, maybe the internet. Metrics are for the operator: they show traffic per upstream, error rates and capacity, which is reconnaissance for an attacker.
- The API port has no whole-request timeouts (streams run for minutes). The metrics server has short read and write timeouts, and the handler admits at most four scrapes at once.
- A metrics route on the API mux would need its own auth story. A loopback listener needs none, and exposing it is an explicit choice, as with `SENTINEL_ADDR`.

`off` disables metrics: no listener, no Recorder. The address must be `host:port` with a numeric port, and not the API's port. The metrics listener binds before the API's and logs `metrics listening`, never `listening`, which `make bench-gateway` waits for.

### The cardinality rule

Every attribute value comes from a closed set fixed by code or by configuration: the upstream names in `SENTINEL_UPSTREAMS`, the protocol, the status class (`2xx`, `4xx`, `5xx`, with `499` on its own), the failure phase, the stage, the rate-limit outcome. Never the client key or its digest, the model, the path, or anything else a client chooses.

Each distinct label value is a new time series that Prometheus keeps in memory and on disk. A label taken from the request lets any client mint series at will, one per request, until the monitoring system runs out of memory: a denial of service on the monitoring, and with it the loss of visibility exactly when it matters. `RequestInfo` doesn't even carry the model or the key, and anything outside a closed set is recorded as `other`. A test sends 1,000 requests with distinct models and keys, accepted and rejected, and checks that no series appears.

Attribute sets are built once at startup, per upstream, status class and phase, so recording a request is a map lookup and eight measurements, with no allocation. The benchmark is about 1.5 µs per request on the bench machine (i7-7700), next to a hot path measured in milliseconds.

### Instruments and buckets

| Instrument (OpenTelemetry) | Prometheus name | Kind | Attributes |
| --- | --- | --- | --- |
| `sentinel.requests` | `sentinel_requests_total` | counter | upstream, protocol, status_class, phase (on failure) |
| `sentinel.request.duration` | `sentinel_request_duration_seconds` | histogram | upstream, protocol, status_class |
| `sentinel.stage.duration` | `sentinel_stage_duration_seconds` | histogram | stage |
| `sentinel.gateway.overhead` | `sentinel_gateway_overhead_seconds` | histogram | upstream, protocol |
| `sentinel.ratelimit.decisions` | `sentinel_ratelimit_decisions_total` | counter | outcome |
| `sentinel.auth.rejections` | `sentinel_auth_rejections_total` | counter | |
| `sentinel.admission.in_use`, `.limit` | `sentinel_admission_in_use`, `_limit` | gauge | |
| `sentinel.ratelimit.breaker.open` | `sentinel_ratelimit_breaker_open` | gauge, 0 or 1 | |

The exporter's translation is pinned (`UnderscoreEscapingWithSuffixes`): dots become underscores, `s` becomes `_seconds`, counters get `_total`. The dashboard queries the Prometheus names, and a test scrapes the exporter and checks every name and label value the dashboard uses.

Bucket boundaries, in seconds:

- **Request duration, 50 ms to 600 s** (0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600). From a fast error to a non-streaming generation at the 10-minute first-byte timeout.
- **Stage duration, 100 µs to 10 s** (16 boundaries). A healthy Redis call is well under a millisecond and times out at 50 ms; an upstream's first byte takes up to seconds. Anything longer lands in `+Inf`, where the request histogram still resolves it.
- **Gateway overhead, 50 µs to 1 s** (14 boundaries). bench/RESULTS.md measured about 1 ms or less at p50, so the resolution sits below that; a second of overhead is a bug, not a percentile.

Overhead is total minus read body, upstream time to first byte and stream, recorded only for requests that reached the upstream; for an early rejection it would be the whole request. The rate-limit call counts as overhead: it is the gateway's own work.

## Consequences

- 19 more modules linked into the gateway on Linux, and a 3.4 MiB larger binary. Each new release of OpenTelemetry or client_golang is another update to review. The README's dependencies badge says so.
- A second port to think about when deploying. It is loopback by default and never published by compose.
- `sentinel.requests` counts requests that passed authentication; 401s are counted by `sentinel.auth.rejections`, and unrouted requests (404, an unclean path's 400) by neither.
- The `unauthorized` WARN from `RequireKey` is now rate-limited like the fail-open warning, at most once every 10 s with a `suppressed` count, since the counter carries the rate. This closes the log-flood item of #6.
- No traces yet. They will reuse this SDK: a tracer provider in `internal/telemetry`, spans around the same stages, and exemplars linking histogram buckets to traces.
- No Go runtime metrics (goroutines, GC, memory). The OpenTelemetry runtime instrumentation is a separate contrib module and can come with traces.
