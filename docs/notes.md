# Development notes

Newest entries first. Short: what was done, what was decided, what's next.

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
  refuses unless every base_url is the mock. gateway-bench and make up gain
  a custom upstream, mockcompat, and k6 takes BENCH_PREFIX.
- ADR 0003. Tests: names, missing and bad variables, unlisted and
  built-in variables, no secrets in errors or logs, two upstreams of one
  protocol each reaching only its own server and key, unprefixed routes
  only for built-ins, 404 + close on an unknown prefix, one Admission
  across prefixes, the log fields, and the guard's matching.

### Decided
- Route by path prefix, not by model name. The path is fixed by
  configuration and fails closed with a 404; a model table would be
  client-driven and always out of date.
- No defaults for custom upstreams. A guessed default could send one
  provider's key to another's API.
- Names are [a-z][a-z0-9]{0,31}, no hyphens or underscores, so each
  variable maps back to one name. "v1" is reserved.
- Environment variables, not a config file, until upstreams live in
  Postgres per tenant.
- A bad PROTOCOL value is not echoed in its error, since it could be a
  mispasted key. Bad names in SENTINEL_UPSTREAMS still are, as before.
- The default-host CA warning stays for the built-ins only.

### Learned
- An env-var bench guard has to know every upstream in advance; the
  gateway's own "upstream enabled" lines are what it will actually call.
- On Windows, make runs a recipe line with no shell syntax directly, not
  through sh, so a fake docker script on PATH is not enough to test a
  recipe; the compose command had to be overridden as well.

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
