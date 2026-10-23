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
- Keep it simple: stdlib first, add a dependency only with a reason.
- Every change comes with tests.
- Never commit secrets. API keys come only from environment variables.

## Commands
- go run ./cmd/gateway (needs the env vars below)
- go test ./...

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
  default 128. Also sizes the upstream idle connection pool. Must be a
  positive integer; the gateway refuses to start otherwise.
- SENTINEL_UPSTREAM_CA_FILE (optional): PEM file of extra CA certificates
  trusted for the upstream connection, added to the system pool (not
  replacing it). Every PEM block must be a valid certificate; the gateway
  refuses to start if the file is set but missing, holds none, or holds
  anything else. It warns when the CAs would apply to the default upstream
  and when a cert is not a CA. It does not relax the https-only rule.
  `go run ./cmd/devcerts` writes a dev CA and server cert to ./certs
  (gitignored) for running cmd/mockupstream with -tls-cert/-tls-key.

## Git workflow
- Branch from dev. Open pull requests against dev, never main.
- main receives only milestone merges from dev.
- Merge dev into main with a merge commit, never squash.
