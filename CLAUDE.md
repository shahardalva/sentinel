# Sentinel

Security gateway for LLM apps and MCP agents. A proxy in front of LLM
providers that detects prompt injection, redacts secrets/PII, enforces
tool policies, and writes an audit log.

## Stack
- Gateway: Go, net/http, chi, log/slog
- Upstreams: Anthropic first, OpenAI later, behind a Provider interface
- Later: Redis, Postgres (pgx, sqlc, goose), Kafka (franz-go), ClickHouse,
  Python detector service over gRPC

## Principles
- The hot path must be fast and measurable. Every stage records its latency.
- Keep it simple: stdlib first, add a dependency only with a reason.
- Every change comes with tests.
- Never commit secrets. API keys come only from environment variables.

## Commands
- go run ./cmd/gateway
- go test ./...
