# Sentinel

Security gateway for LLM apps and MCP agents. A proxy in front of LLM
providers that detects prompt injection, redacts secrets/PII, enforces
tool policies, and writes an audit log.

## Stack
- Gateway: Go, net/http, log/slog
- Upstreams: Anthropic first, OpenAI later, behind a Provider interface
- Later: Redis, Postgres (pgx, sqlc, goose), Kafka (franz-go), ClickHouse,
  Python detector service over gRPC

## Principles
- The hot path must be fast and measurable. Every stage records its latency.
- Keep it simple: stdlib first, add a dependency only with a reason. The
  only one so far is go-redis (docs/adr/0002-rate-limiting.md); miniredis
  is test-only.
- Every change comes with tests.
- Never commit secrets. API keys come only from environment variables.

## Commands
- go run ./cmd/gateway (needs the env vars below), or make run
- go test ./... (make test adds -race; make vet, make fmt). make test-redis
  also runs the rate-limit tests on a throwaway redis:7-alpine container;
  SENTINEL_TEST_REDIS_URL enables them by hand, and CI sets it.
- make (or make help) lists the targets.
- make up: compose stack with gateway -> mockupstream over HTTPS, plus
  redis for rate limiting. Runs make certs first, which rewrites ./certs when a
  file is missing or server.pem expires within 7 days, and recreates the
  containers. Gateway on 127.0.0.1:8081 only. make up-real: gateway
  against the real API. make down, make logs.
- make bench: k6 in a container against the mock directly (after make up).
  make bench-gateway: k6 through gateway-bench, a separate gateway fixed to
  the mock with a dummy upstream key; it refuses to run if that container's
  ANTHROPIC_BASE_URL is not the mock. DURATION_S and STREAM
  pass through.
- Compose without make: docker compose up (real), or add
  -f docker-compose.yml -f docker-compose.mock.yml --profile mock (mock).

## Configuration (environment variables)
- ANTHROPIC_API_KEY (required): the gateway's own upstream key. Clients never see it.
- SENTINEL_API_KEYS (required): comma-separated client keys. The gateway
  refuses to start without at least one. Clients send a key as `x-api-key`
  or `Authorization: Bearer <key>`; anything else gets a 401.
- SENTINEL_ADDR (optional): listen address, default 127.0.0.1:8081
  (loopback only). Set it explicitly to expose the gateway on a network.
- ANTHROPIC_BASE_URL (optional): upstream override, default https://api.anthropic.com.
  Must be https (plain http only for a loopback host); the gateway refuses
  to start otherwise.
- SENTINEL_MAX_CONCURRENT (optional): cap on requests in flight upstream,
  default 128. Also sizes the upstream idle connection pool and the Redis
  connection pool. Must be a
  positive integer; the gateway refuses to start otherwise.
- SENTINEL_UPSTREAM_CA_FILE (optional): PEM file of extra CA certificates
  trusted for the upstream connection, added to the system pool (not
  replacing it). Every PEM block must be a valid certificate; the gateway
  refuses to start if the file is set but missing, holds none, or holds
  anything else. It warns when the CAs would apply to the default upstream
  and when a cert is not a CA. It does not relax the https-only rule.
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

Under Docker Compose the gateway reads ANTHROPIC_API_KEY and
SENTINEL_API_KEYS from .env (copy .env.example, which leaves them empty;
.env is gitignored). Compose sets SENTINEL_ADDR=0.0.0.0:8081 and
SENTINEL_REDIS_URL=redis://redis:6379/0 in the container and publishes the
port on 127.0.0.1 only. gateway-bench uses Redis database 1 with
SENTINEL_RATE_LIMIT_RPM=1000000, so the bench includes the Redis call but
never gets a 429. The mock override sets ANTHROPIC_BASE_URL=
https://mockupstream:9090, SENTINEL_UPSTREAM_CA_FILE=/certs/ca.pem and a
dummy ANTHROPIC_API_KEY. The gateway, gateway-bench and k6 mount only
certs/ca.pem; certs-init and mockupstream mount certs/ read-only. certs-init
copies the server key into a volume as a file owned by the nonroot user
(65532) with mode 0400, since devcerts writes it 0600 for the host user; the
volume itself stays root-owned. BENCH_API_KEY in .env (optional, default
"bench") is the only client key gateway-bench accepts.

## Git workflow
- Branch from dev. Open pull requests against dev, never main.
- main receives only milestone merges from dev.
- Merge dev into main with a merge commit, never squash.
