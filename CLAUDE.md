# Sentinel

Security gateway for LLM apps and MCP agents. A proxy in front of LLM
providers that detects prompt injection, redacts secrets/PII, enforces
tool policies, and writes an audit log.

## Stack
- Gateway: Go, net/http, log/slog
- Upstreams: two protocols, Anthropic and OpenAI-compatible
  (internal/provider/openai), behind a Provider interface; any number of
  named upstreams per protocol (docs/adr/0003-named-upstreams.md)
- Later: Redis, Postgres (pgx, sqlc, goose), Kafka (franz-go), ClickHouse,
  Python detector service over gRPC

## Principles
- The hot path must be fast and measurable. Every stage records its latency.
- Keep it simple: stdlib first, add a dependency only with a reason. The
  only one so far is go-redis (docs/adr/0002-rate-limiting.md); miniredis
  is test-only.
- Every change comes with tests.
- Never commit secrets. API keys come only from environment variables.
- Claude Code never runs docker, docker compose, or make targets that start
  containers, and never reads .env. Ask the user to run them.

## Commands
- go run ./cmd/gateway (needs the env vars below), or make run
- go test ./... (make test adds -race; make vet, make fmt). make test-redis
  also runs the rate-limit tests on a throwaway redis:7-alpine container;
  SENTINEL_TEST_REDIS_URL enables them by hand, and CI sets it.
- make (or make help) lists the targets.
- make up: compose stack with gateway -> mockupstream over HTTPS, plus
  redis for rate limiting. The gateway serves both protocols there, with
  anthropic, openai and the named upstream mockcompat (openai) all on the
  mock. Runs make certs first, which rewrites ./certs when a
  file is missing or server.pem expires within 7 days, and recreates the
  containers. Gateway on 127.0.0.1:8081 only. make up-real: gateway
  against the real APIs in SENTINEL_UPSTREAMS from .env (default
  anthropic). make down, make logs.
- make bench: k6 in a container against the mock directly (after make up).
  make bench-gateway: k6 through gateway-bench, a separate gateway fixed to
  the mock for all its upstreams (anthropic, openai, mockcompat) with dummy
  upstream keys. It checks resolved config, not env vars: it waits (about
  15s) for gateway-bench's "listening" line, then cmd/benchguard refuses
  to run unless there is at least one "upstream enabled" line and every
  base_url is https://mockupstream:9090 or starts with
  https://mockupstream:9090/. BENCH_API
  ("anthropic" or "openai"), BENCH_PREFIX (e.g. /mockcompat, prepended to
  the path), DURATION_S and STREAM pass through.
- Compose without make: docker compose up (real), or add
  -f docker-compose.yml -f docker-compose.mock.yml --profile mock (mock).

## Configuration (environment variables)
- SENTINEL_UPSTREAMS (optional): comma-separated upstreams to serve: the
  built-ins anthropic (POST /v1/messages) and openai
  (POST /v1/chat/completions), and named upstreams; default anthropic.
  Names match ^[a-z][a-z0-9]{0,31}$ and "v1" is reserved; a bad name, a
  duplicate, or a list with no names stops startup. Every enabled upstream
  gets POST /<name>/v1/messages or /<name>/v1/chat/completions by protocol;
  only the built-ins also get the unprefixed route. A route exists only for
  an enabled upstream; all routes share the client keys, the rate limiter
  and one proxy.Admission. One INFO line per enabled upstream logs its
  name, protocol and base URL, never the key. The request log has
  "upstream" (the name, via proxy.Options.Upstream) and "provider" (the
  protocol's Name()).
- SENTINEL_UPSTREAM_<NAME>_PROTOCOL, _BASE_URL, _API_KEY (all required for
  a listed named upstream, NAME upper-cased): "anthropic" or "openai", the
  base URL (no default; same https rule and error rules as the built-ins),
  and the gateway's key for it. Any SENTINEL_UPSTREAM_ANTHROPIC_* or
  SENTINEL_UPSTREAM_OPENAI_* variable stops startup. Variables of an
  unlisted NAME are not validated or used; one WARN per name, no values.
  loadConfig takes getenv and environ, so tests stay hermetic.
- ANTHROPIC_API_KEY (required when anthropic is listed): the gateway's own
  upstream key. Clients never see it.
- OPENAI_API_KEY (required when openai is listed): the gateway's own key for
  the OpenAI-compatible upstream. A key set for an unlisted upstream is not
  used, and a WARN at startup says so. An enabled upstream's key with
  leading or trailing whitespace or a control character stops startup; the
  error names the variable, never the key.
- SENTINEL_API_KEYS (required): comma-separated client keys. The gateway
  refuses to start without at least one. Clients send a key as `x-api-key`
  or `Authorization: Bearer <key>`; anything else gets a 401.
- SENTINEL_ADDR (optional): listen address, default 127.0.0.1:8081
  (loopback only). Set it explicitly to expose the gateway on a network.
- ANTHROPIC_BASE_URL (optional): upstream override, default https://api.anthropic.com.
  Must be https (plain http only for a loopback host); the gateway refuses
  to start otherwise. Base URL errors name at most the scheme and hostname,
  never the URL, which may hold a key. Other provider construction errors
  read "upstream <name>: ...".
- OPENAI_BASE_URL (optional): OpenAI-compatible upstream, default
  https://api.openai.com/v1. It includes /v1, like the OpenAI SDKs' own
  variable; the provider appends /chat/completions. Same https rule. For
  Ollama on the host: http://127.0.0.1:11434/v1.
- SENTINEL_MAX_CONCURRENT (optional): cap on requests in flight upstream,
  default 128. Also sizes the upstream idle connection pool and the Redis
  connection pool. Must be a
  positive integer; the gateway refuses to start otherwise.
- SENTINEL_UPSTREAM_CA_FILE (optional): PEM file of extra CA certificates
  trusted for every enabled upstream, added to the system pool (not
  replacing it). Every PEM block must be a valid certificate; the gateway
  refuses to start if the file is set but missing, holds none, or holds
  anything else. It warns for each enabled upstream left on its public
  default host (api.anthropic.com, api.openai.com) and when a cert is not
  a CA. It does not relax the https-only rule.
  `go run ./cmd/devcerts` writes a dev CA and server cert to ./certs
  (gitignored) for running cmd/mockupstream with -tls-cert/-tls-key.
- SENTINEL_REDIS_URL (optional): redis:// or rediss:// URL for per-key rate
  limiting. Unset means no rate limiting, logged at startup. A malformed URL
  stops startup (the error never repeats the URL, which may hold a
  password); an unreachable Redis at startup only warns.
- SENTINEL_RATE_LIMIT_RPM (optional): requests per minute per client key,
  default 60, also the burst. Must be a positive integer.
- SENTINEL_RATE_LIMIT_FAIL_MODE (optional): "open" (default) or "closed",
  for when Redis can't answer within 50 ms. Open lets the request through
  and logs a WARN at most every 10s; closed answers 503. Anything else stops
  startup. After 5 Redis failures in a row a circuit breaker skips Redis
  for 5s and applies the fail mode at once, then lets one request try
  again; opening and closing are logged. Only that probe can close it.
  When every Redis connection stays busy for the 50 ms, the request gets
  the fail mode but the breaker doesn't count it. A client that disconnects
  during the check is logged as 499, not as a Redis failure.

Under Docker Compose the gateway reads ANTHROPIC_API_KEY, OPENAI_API_KEY,
OPENAI_BASE_URL, SENTINEL_UPSTREAM_<NAME>_* and SENTINEL_API_KEYS from .env
(copy .env.example, which leaves the keys empty; .env is gitignored), and
SENTINEL_UPSTREAMS too, defaulting to anthropic. Compose sets SENTINEL_ADDR=0.0.0.0:8081 and
SENTINEL_REDIS_URL=redis://redis:6379/0 in the container and publishes the
port on 127.0.0.1 only. gateway-bench uses Redis database 1 with
SENTINEL_RATE_LIMIT_RPM=1000000, so the bench includes the Redis call but
never gets a 429. The mock override and gateway-bench set
SENTINEL_UPSTREAMS=anthropic,openai,mockcompat, ANTHROPIC_BASE_URL=
https://mockupstream:9090, OPENAI_BASE_URL=https://mockupstream:9090/v1,
mockcompat as protocol openai on https://mockupstream:9090/v1,
SENTINEL_UPSTREAM_CA_FILE=/certs/ca.pem and dummy keys for all three
(mock-not-a-real-key), so load tests never reach a real API. The gateway, gateway-bench and k6 mount only
certs/ca.pem; certs-init and mockupstream mount certs/ read-only. certs-init
copies the server key into a volume as a file owned by the nonroot user
(65532) with mode 0400, since devcerts writes it 0600 for the host user; the
volume itself stays root-owned. BENCH_API_KEY in .env (optional, default
"bench") is the only client key gateway-bench accepts.

## Git workflow
- Branch from dev. Open pull requests against dev, never main.
- main receives only milestone merges from dev.
- Merge dev into main with a merge commit, never squash.
