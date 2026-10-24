# Development notes

Newest entries first. Short: what was done, what was decided, what's next.

## 2026-10-24

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
