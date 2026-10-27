# Development notes

Newest entries first. Short: what was done, what was decided, what's next.

## 2026-10-27: refreshable key source (Postgres keys, PR 1a)

### Done
- proxy.KeySource returns a *proxy.KeySet, an immutable set of SHA-256
  digests (NewKeySet copies). Auth asks it once per request, so a request
  is checked against one whole set. Digest compare, the constant-time loop
  over every digest and clientID (hex digest, so rate-limit buckets) are
  unchanged. A nil or empty set rejects every request. NewAuthSource takes
  a KeySource; NewAuth and RequireKey keep taking []string and wrap it.
  internal/proxy imports neither internal/keys nor any DB package.
- internal/keys: Parse (SENTINEL_API_KEYS's rules, moved from main.go) and
  Static; Refreshing, which fetches digests on an interval and swaps them
  in with an atomic.Pointer. The first fetch must succeed or the
  constructor returns its error and starts nothing. A failed refresh keeps
  the last good set, is recorded and logged at WARN through
  proxy.LogThrottle (10 s). A fetch cut off by
  shutdown is not a failure. Done() closes when the loop stops on ctx
  cancel. Becoming empty logs one WARN.
- proxy.Recorder gained RecordKeySetSize and RecordKeyRefresh. Telemetry:
  sentinel_auth_keys, sentinel_auth_key_refresh_errors_total and
  sentinel_auth_key_refresh_age_seconds, no labels. Static reports only the
  size, so it has no age or error series; a refreshing source's error
  series exists at 0 from its first success.
- main.go uses keys.Static; Refreshing has no caller until PR 1b.
- Tests under -race: a removed key rejected after the next refresh, swaps
  racing concurrent requests, a failed refresh keeping the set, empty sets,
  cancel (also mid-fetch) stopping the loop, a failed first fetch starting
  nothing. The refresh tests run in testing/synctest, which also fails on a
  leaked goroutine. Existing auth tests unchanged.

- After review (go-reviewer): the ticker is now a timer reset after each
  refresh returns, so a hung fetch is followed by a full Interval of rest,
  not another fetch at once. Config.FetchTimeout (default
  min(Interval/2, 10s), must be positive and below Interval) bounds every
  fetch, the first too: a hung store fails startup with "the key store did
  not answer in time". refresh checks ctx before fetching; a ctx already
  done fails NewRefreshing before any fetch. Keys() on a nil Static or
  Refreshing returns nil and rejects. Config.Now drives the warning
  throttle, so synctest tests run it after its base. proxy.DigestKeys is
  the one place keys become digests. Tests added for each, written to fail
  first for the hung fetches and the fetch after cancel.

### Decided
- logThrottle stays in internal/proxy; LogThrottle exports it with Allow
  for internal/keys, which already imports proxy. Moving it to a package
  of its own would have meant changing the existing auth tests. A known
  smell: revisit if a third user appears.
- An empty first fetch is accepted: the gateway starts and fails closed,
  rejecting every request, with one WARN. Refusing to start would turn
  "every key revoked" into "gateway down".
- One key source per process, so the key gauges have no source label by
  design; a second source would overwrite the first's values.
- Review: go-reviewer with a focused brief on every PR; the multi-agent
  code-review skill on each dev->main release diff.

### Next
- PR 1b: Postgres fetch (pgx, sqlc, goose) behind keys.Refreshing, ADR 0005.
  - Sanitize pgx errors in the Fetch (connect errors carry user, database
    and host:port) and test that no connection string reaches a log line.
  - With a large key set, look the digest up in a map instead of the
    linear constant-time loop; record the trade-off in the ADR.
  - Alert on sentinel_auth_key_refresh_age_seconds, not only on refresh
    errors: a Fetch that ignores ctx records no failure.
- Dashboard panels for the key metrics.

## 2026-10-25: fail-open windows in a bench

### Done
- bench/RESULTS.md has a caveat: a make bench-gateway run, watched with
  the new metrics, had 4,058 of about 47k requests (8.6%) fail_open.
  gateway-bench logged "redis not reachable at startup" and the breaker
  opened four times during the 100 and 500 req/s stages. The log's
  fail-open WARN lines plus their suppressed counts give 4,054; the last
  batch is never logged. Tracked in #42.

### Learned
- HTTP statuses don't show a fail-open window: a fail-open request goes
  ahead and is answered like any other, and while the breaker is open it
  skips the Redis call. Earlier overhead figures may be optimistic, mostly
  at 100 req/s.
- Grafana's runtime plugin preinstall broke the dashboard on a read-only
  root: its update removed the bundled Prometheus plugin and couldn't
  write the new one. Preinstall is now off; the image's copy loads.

### Next
- After every bench, check
  sentinel_ratelimit_decisions_total{outcome="fail_open"}, not only HTTP
  statuses. #42 is to make make bench-gateway check it and reject the run.

## 2026-10-25: OpenTelemetry metrics

### Done
- proxy.Recorder: the Handler reports one RequestInfo per request from the
  deferred block that writes the log line (upstream, protocol, status,
  failure phase, rate-limit outcome, which stages ran and their
  durations, total). Options.Recorder nil costs a nil check. The log line
  and the wire bytes are unchanged; internal/proxy imports no OTel.
- proxy.Auth, built once by NewAuth and shared by every route (RequireKey
  stays as the one-route form). It reports each rejection to the Recorder
  and logs "unauthorized" at most once per 10 s with a suppressed count,
  through the same throttle as the fail-open warning. From #6.
- Admission.InUse and Limit, Limiter.BreakerOpen, for observable gauges.
- internal/telemetry: OTel SDK and the Prometheus exporter on a registry
  of its own, translation pinned. Requests, request duration, stage
  duration, gateway overhead, rate-limit decisions, auth rejections,
  admission in use and limit, breaker open. Attribute sets precomputed
  per upstream, status class and phase; recording a request is 0 allocs,
  about 1.5 µs.
- SENTINEL_METRICS_ADDR, default 127.0.0.1:9464, GET /metrics only, on its
  own listener; "off" disables. Binds before the API and logs "metrics
  listening"; shuts down after it. run() split into loadConfig and
  serve(ctx, logger, cfg), so tests start both listeners on port 0.
- Compose: prom/prometheus:v3.15.1 scraping gateway:9464 and
  gateway-bench:9464; grafana/grafana:13.2.4 on 127.0.0.1:3000 with
  anonymous Viewer, login form and basic auth off, a provisioned data
  source and dashboard (observability/). Both read_only, cap_drop ALL,
  no-new-privileges.
- ADR 0004. Tests: each proxy path recorded once with the right phase,
  status and outcome; the auth WARN throttled across routes; instruments
  and attributes through a ManualReader; overhead math; 1,000 distinct
  keys and models adding no series; the dashboard's names and label values
  present in a real scrape; the metrics listener, 404 for /metrics on the
  API port, off, and bad or taken addresses; benchguard with the new lines.
- Smoke test (make up, k6 at 1000 req/s): k6, Prometheus and the log all
  counted 47959 requests, 47811 2xx and 148 503 at admission. Every
  Grafana panel failed: "datasource prometheus was not found".
- Review fixes: a provider panic is recorded and logged as a failure (500
  unless a status was sent, with the stack), then re-raised. A metrics
  Serve error is logged and the API keeps serving. Default port 9464
  (9091 is the Pushgateway's). Metrics and API ports compare as numbers on
  any host. logThrottle measures from a monotonic base. Grafana: no
  runtime plugin installs or updates, no initial admin, no Gravatar, no
  external snapshots; Prometheus keeps at most 1 GB.

### Decided
- OTel over client_golang alone or hand-written exposition: one
  vendor-neutral API, one SDK for traces next, and the exporter can become
  OTLP without touching a recording call. The Prometheus exporter is built
  on client_golang, so client_golang is a direct dependency either way.
- Labels only from closed sets, and anything else becomes "other".
  RequestInfo doesn't even carry the model or the key.
- 401s are counted by sentinel.auth.rejections, not sentinel.requests; the
  Handler never sees them.
- Overhead is recorded only for forwarded requests; for a 429 it would be
  the whole request. The Redis call counts as overhead.
- A client gone during the rate-limit check is outcome not_checked: no
  decision was made and no fail mode applied.

### Learned
- Grafana 13.2 ships the Prometheus data source as a plugin, not core: a
  copy bundled in the image (data/plugins-bundled) plus a background
  "preinstall" that updates it from grafana.com at startup. With a
  read-only root the update removed the plugin and couldn't write the new
  one. plugins.preinstall_disabled turns the whole preinstall off; the
  bundled copy still loads, since paths.bundled_plugins is always on the
  plugin path (pkg/setting/setting.go, v13.2.4).
- Re-panicking from a deferred recover loses the panic site in net/http's
  log, so the handler logs debug.Stack() before re-panicking.
- Windows' monotonic clock is coarse enough that a fast stage measures 0,
  so RequestInfo says which stages ran (Ran) instead of relying on
  durations above zero.
- Passing a precomputed metric.WithAttributeSet option as a lone variadic
  argument still allocates the slice, 16 B per call (measured); passing a
  precomputed []metric.AddOption with ... allocates nothing.

### Next
- Traces on the same SDK, with exemplars linking duration buckets to them.
- Go runtime metrics (contrib runtime instrumentation).
- Measure the recording cost under load in make bench-gateway.
- A client that disconnects while uploading its body is logged and
  recorded as 400 read_body, not 499. Tell it apart from a malformed body.

## 2026-10-25: provider hardening

### Done
- Provider contract (#5): a nil error from Forward means a non-nil
  Response with a non-nil Body and a status from 200 to 599. The Handler
  checks it and answers a breach with 502 "upstream unavailable", logged
  with phase upstream, closing a Body it got. No panic.
- RequestError.Status: zero means 400, other 4xx values are honored,
  anything else becomes 400.
- The Handler strips hop-by-hop response headers (and any header named in
  Connection) itself, whatever the provider's allowlist lets through.
- Upstream error bodies (#33): each provider reads a non-2xx body up to
  64 KiB and re-encodes its own error shape with the message's account
  identifiers redacted (org-..., proj_..., UUIDs); request IDs stay.
  Anything else is replaced with the gateway's "upstream error" at the
  same status. 2xx bodies are never read. Shared ReadErrorBody and
  RedactAccountIDs in internal/provider/upstream.
- upstream.NewClient no longer panics when http.DefaultTransport has been
  replaced (from #6); it falls back to a fresh transport with Go's
  documented defaults.
- Review fixes: an upstream 1xx (Go hands back a 101) is a Forward error,
  502, with no body read. ReadErrorBody takes the context, closes the body
  when it ends (context.AfterFunc), and has its own 10 s deadline, a 502
  logged as "upstream error body not received in time" instead of the
  header timeout's 504. The Handler accepts only 2xx, 4xx and 5xx; a 300,
  a 304 or a 3xx without Location is a 502. copyEndToEnd writes canonical
  keys, so a provider's x-request-id can't sit beside the gateway's.
  RequestError's 401, 407, 429 and 499 become 400. Redaction is
  case-insensitive, catches IDs after "_" or a digit, and covers sk-...,
  wrkspc_..., apikey_... and 32-hex UUIDs. OpenAI's numeric code keeps
  its digits (json.Number).

### Decided
- Sanitizing stays in the providers, not the core: only a provider knows
  its error shape, and replacing every upstream error with a generic one
  would hide messages developers need.
- Re-encode rather than edit in place: unknown fields are dropped, so an
  identifier in a field nobody looked at can't slip through. OpenAI's
  param and code are kept, and a numeric code from a compatible server
  too.
- 401/403 bodies are sanitized as well, though the core replaces them;
  the provider rule is simply "every non-2xx body".
- A failed read of an error body (connection reset, its own timeout) is a
  Forward error, so the client gets the core's 502 (499 if it left), not a
  made-up body at the upstream's status.
- A RequestError can't become a 5xx: a provider rejecting the client's
  request must not look like a gateway failure to SDK retry logic or to
  the log level.

### Learned
- encoding/json escapes <, > and & by default, so a re-encoded message
  like "max_tokens: 100000 > 64000" changed bytes; the re-encoder turns
  that off.
- A 101 response turns the body into the raw connection, and cancelling
  the request no longer stops a read on it: the first error-body reader
  hung forever on one. Every stage that reads from the network needs its
  own deadline, enforced by closing what it reads from, not only a
  context.

### Next
- Sanitize error events inside an SSE stream after a 200 (documented as a
  known limitation).
- Show the operator the sanitized upstream reason when the gateway's own
  key is rejected (401/403). The core drops that body today; it needs a
  Response field for the provider to pass the reason in.
- The other nits in #6: timer Stop races, flushing only for
  text/event-stream, the query string, sampling the auth WARN.

## 2026-10-24: named upstreams

### Done
- Protocol and upstream are split in cmd/gateway: a protocol (anthropic,
  openai) is a path and a Provider constructor; an upstream is a name, a
  protocol, a base URL and a key. The built-ins keep ANTHROPIC_* and
  OPENAI_*. Any other name in SENTINEL_UPSTREAMS reads
  SENTINEL_UPSTREAM_<NAME>_PROTOCOL, _BASE_URL and _API_KEY, all required.
- Every upstream gets POST /<name>/v1/messages or
  /<name>/v1/chat/completions; the built-ins keep the unprefixed routes
  too. Same client keys, rate limiter and Admission for all.
- The one change in internal/proxy: Options.Upstream, logged as "upstream"
  next to "provider". Defaults to the provider's Name().
- loadConfig takes environ as well as getenv, to find
  SENTINEL_UPSTREAM_<NAME>_* variables for unlisted names (one WARN per
  name, no values) and SENTINEL_UPSTREAM_ANTHROPIC_* / _OPENAI_* (an
  error). The startup line also logs the protocol now.
- make bench-gateway checks what gateway-bench resolved: it waits for
  "listening", then cmd/benchguard reads the "upstream enabled" lines and
  refuses unless each has exactly one base_url equal to the mock's
  scheme://host:port. gateway-bench and make up gain
  a custom upstream, mockcompat, and k6 takes BENCH_UPSTREAM (a bare name).
- ADR 0003. Tests: names, missing and bad variables, unlisted and
  built-in variables, no secrets in errors or logs, two upstreams of one
  protocol each reaching only its own server and key, unprefixed routes
  only for built-ins, 404 + close on an unknown prefix, one Admission
  across prefixes, the log fields, and the guard's matching.
- Review fixes: upstream clients refuse redirects (502, logged as
  "upstream redirect refused"); a request path that isn't clean gets a
  plain-text 400 with Connection: close before the mux; "upstream
  enabled" and the CA warning log only scheme://host[:port]; the CA
  warning covers any upstream on a public default host;
  SENTINEL_UPSTREAM_* names match ignoring case; unknown suffixes on a
  listed upstream warn; "listening" is logged only after the bind;
  benchguard fails closed on unreadable or duplicate-key lines.
- Forward errors no longer carry the request URL: upstream.StripURL drops
  it from http.Client's *url.Error, keeping the cause wrapped. The log
  read `Post "https://host/k/<token>/v1/...": ...`, so a token in a base
  URL's path, or a refused redirect's target, reached the request log.

### Decided
- Route by path prefix, not by model name. The path is fixed by
  configuration and fails closed with a 404; a model table would be
  client-driven and always out of date.
- No defaults for custom upstreams. A guessed default could send one
  provider's key to another's API.
- Names are [a-z][a-z0-9]{0,31}, no hyphens or underscores, so each
  variable maps back to one name. "v1" and "ca" are reserved.
- Environment variables, not a config file, until upstreams live in
  Postgres per tenant.
- A bad PROTOCOL value is not echoed in its error, since it could be a
  mispasted key. Bad names in SENTINEL_UPSTREAMS still are, as before.
- The default-host CA warning applies to any upstream on api.anthropic.com
  or api.openai.com, built-in or custom.

### Learned
- An env-var bench guard has to know every upstream in advance; the
  gateway's own "upstream enabled" lines are what it will actually call.
- On Windows, make runs a recipe line with no shell syntax directly, not
  through sh, so a fake docker script on PATH is not enough to test a
  recipe; the compose command had to be overridden as well.
- Git Bash rewrote BENCH_PREFIX=/mockcompat into a Windows path, and
  every request got 404 in under 1 ms. Inputs should not start with "/".
- Counting log lines by "upstream" also matches the startup line; filter
  on "msg":"request" first.
- The review found two problems that predate this branch and that named
  upstreams make easier to reach: the upstream client followed redirects
  and leaked X-Api-Key, and unclean paths hung before auth.

### Next
- Per-upstream CA file, then per-upstream rate limits.
- Upstream error-body sanitization, one provider per PR.
- A 401 message that doesn't say x-api-key on the OpenAI route.
- Per-key concurrency cap.

## 2026-10-24: OpenAI-compatible protocol

### Done
- New package internal/provider/openai: POST {base}/chat/completions with
  the gateway's bearer key. Default base https://api.openai.com/v1, with
  /v1 in it as in the OpenAI SDKs. Uses internal/provider/upstream for the
  base URL rule and the client.
- No file under internal/proxy changed. The seams from the refactor were
  enough: Provider, ErrorFormatter, shared Admission.
- SENTINEL_UPSTREAMS picks the upstreams (default anthropic); each one's
  key is required only when it is listed. A route is registered only for
  an enabled upstream. cmd/gateway builds the mux in newMux, which tests
  call, with one Admission and one rate limiter for every route.
- mockupstream serves /v1/chat/completions too, streaming chunks and
  data: [DONE]. k6 takes BENCH_API=openai. make up and gateway-bench enable both
  upstreams on the mock with dummy keys; make bench-gateway checks both
  base URLs before it runs.
- Tests: headers sent upstream, response allowlist, OpenAI error bodies,
  streaming and request IDs through RequireKey and the handler, from an
  external test package; SENTINEL_UPSTREAMS parsing; routes per upstream;
  a 503 on one route while the other holds the only slot.

### Decided
- Forward no client header on the OpenAI route. OpenAI-Organization and
  OpenAI-Project choose who is billed and must come from the gateway, if
  anywhere. OpenAI-Beta and X-Stainless-* have no use upstream worth the
  risk of an allowlist we would have to maintain.
- The upstream's X-Request-Id is dropped from the response and logged as
  upstream_request_id. The client keeps the gateway's own X-Request-Id.
- OpenAI error type and code strings are informative only: OpenAI SDKs
  pick the exception from the HTTP status. Not every value is checked
  against OpenAI's live responses.
- An upstream key set for an unlisted upstream is ignored with a WARN, not
  an error, so one .env can serve several setups.
- The extra-CA warning fires per enabled upstream on its public default
  host, compared by hostname so a trailing slash, a trailing dot or a path
  doesn't hide it.
- Upstream error-body sanitization goes in each provider, in its own PR,
  not in the core. Until then error bodies pass through as-is.
- Review fixes: base URL errors name at most the scheme and hostname, and
  never wrap url.Parse's error, which repeats the URL. Upstream keys with
  edge whitespace or control characters stop startup. Only base URL
  errors are blamed on the base URL variable; other provider errors read
  "upstream <name>: ...".

### Learned
- "OpenAI-compatible" covers most local model servers (Ollama, vLLM), so
  one provider reaches far more than OpenAI. Only loopback gets plain http,
  which suits a local Ollama but not one in another container.
- Azure OpenAI, Bedrock and Vertex differ in auth, not just in base URL, so
  they need providers of their own.
- A load test that returns only 200s does not prove which route it hit.
  Count the gateway's log lines by provider, as was done for
  BENCH_API=openai.
- A 2 s bench right after the container is recreated shed about 8% at
  1000 req/s with 503 at admit, during a machine stall (one Redis call
  took 7.4 ms against a 249 us median). Use 5 s or more for smoke runs.

### Next
- Named upstreams, so OpenAI and a local server can be served side by side.
- Upstream error-body sanitization, one provider per PR.
- A 401 message that doesn't say x-api-key on the OpenAI route. It comes
  from RequireKey, so it means a change in internal/proxy.
- Per-key concurrency cap.

## 2026-10-24: provider seams

### Done
- Refactor ahead of a second protocol and named upstreams. Nothing changes
  on the wire for the Anthropic route.
- The concurrency cap is now a proxy.Admission, created once in
  cmd/gateway and shared by every handler. A handler without one still
  makes its own from MaxConcurrent.
- Error responses are rendered by the protocol: the core has error kinds
  and an ErrorFormatter interface that Provider embeds, and RequireKey
  takes one. The Anthropic provider maps the kinds to its error types.
- A characterization test pins the exact bytes, headers and status of
  every error the gateway emits, from 401 to 504. Written and run green
  before the refactor, unchanged after it.
- New package internal/provider/upstream: base URL rule, loopback check,
  pooled TLS client with extra CAs, header control-character check. It
  does not import internal/proxy. Anthropic's errors keep their text.
- .claude/settings.local.json and .claude/worktrees/ in .gitignore.

### Decided
- One cap for the whole gateway, not one per route. A cap per route would
  multiply the memory bound in ADR 0001 by the number of routes.
- The core owns error meaning, the protocol owns error shape. An OpenAI
  SDK reads error.message from Anthropic's shape, but the type values are
  wrong, and other clients may not parse it at all. The 401 from
  RequireKey is the first thing a misconfigured client sees.
- Programmer errors panic, like http.Handle with a nil handler: a nil
  formatter or a zero-value Admission at construction, an unmatched
  Release when it is called.
- Shared upstream code takes MaxIdleConns from the caller rather than
  importing proxy for the default, so any protocol can use it.

### Learned
- net/http writes the handler's headers sorted, then Date and
  Content-Length, so a raw-bytes golden test is stable once Date and
  X-Request-Id are masked.

### Next
- OpenAI-compatible provider on these seams.
- Named upstreams, each with its own base URL and key.
- Per-key concurrency cap, so one key can't take every upstream slot.

## 2026-10-23

### Done
- Repo made public. README with architecture diagram.
- CI on every PR: gofmt, go vet, go test -race.
- Load test with k6 against a mock upstream (100 ms delay). Median gateway
  overhead about 1 ms or less up to 1000 req/s. Results in bench/RESULTS.md.
- Concurrency cap made configurable (SENTINEL_MAX_CONCURRENT). ADR 0001.
- Private CA support for the upstream (SENTINEL_UPSTREAM_CA_FILE) and
  cmd/devcerts, so the mock runs over HTTPS. The https-only rule is unchanged.
- Docker Compose dev environment and Makefile. make bench-gateway runs only
  against a gateway fixed to the mock and refuses to start otherwise.
- Per-key rate limiting in Redis (PR #25, ADR 0002). A token bucket per
  client key in one Lua script, atomic and on Redis's clock. 429 with
  Retry-After, checked before the body is read. The Redis key is a SHA-256
  digest of the client key, never the key itself. Every Redis call is
  bounded at 50 ms. Fail open by default, fail closed as an option. First
  external dependency: go-redis (miniredis for tests only).
- Circuit breaker in front of Redis: after 5 failures in a row it skips
  Redis for 5 s, then one probe decides whether it closes. A review found it
  could flap when a call already in flight succeeded after it opened; now
  only the probe changes its state. Running out of pooled connections no
  longer counts as a Redis failure.
- Rate-limit tests also run on a real Redis (CI service, make test-redis),
  since miniredis runs Lua with a different interpreter.

### Decided
- Tail spikes in the load test point at the shared test machine; treat p99
  as an upper bound until k6 runs on a separate machine.
- Mock over HTTPS with a private CA, instead of a flag that allows plain http.
- Merge dev into main with a merge commit, never squash. An earlier squash
  split the histories; fixed with a merge -s ours on dev.
- Rate limiting fails open by default: it protects fair use and cost, not
  confidentiality, and fail closed would make Redis as critical as the
  gateway.

### Learned
- Changes to .env need make up again; compose reads it only at start.
- The bench target could have hit the real API with the real key. Guard
  added, and auto-reload stays off on the Anthropic account.
- The Redis call costs a median of 249 µs and a p99 of 1.2 ms in a short
  bench run. A first number, not a full bench result.
- An unsalted digest in Redis can be brute-forced for short client keys.
  Issue #24: derive the bucket key with HMAC and a gateway secret.

### Next
- Per-key concurrency cap, so one key can't take every upstream slot even
  within its rate limit.
- OpenAI provider behind the same interface.

## 2026-10-08

### Done
- Minimal Anthropic proxy in Go, stdlib only, behind a Provider interface.
- Gateway auth, streaming with per-chunk flush, first-byte and idle timeouts,
  header allowlist, resource limits, structured logs with per-stage timings.
- Two rounds of review with a reviewer agent; deferred findings filed as issues.

### Decided
- Gateway binds to 127.0.0.1 by default.
- Running streams are drained on shutdown up to the grace period.
