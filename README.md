# Sentinel

**A security gateway for LLM applications and AI agents.**

Sentinel is a reverse proxy that sits between your application and an LLM provider. Clients point their existing SDK at Sentinel instead of the provider. Sentinel authenticates them, forwards the request with its own upstream credentials, relays the response (including streams) as it arrives, and records a structured log line with per-stage timings for every request.

The goal is a single control point for LLM traffic: who is calling, what is being sent, and what an agent is allowed to do. Detection of prompt injection and sensitive data, policy enforcement, and an audit trail are being built on top of this core.

[![CI](https://github.com/shahardalva/sentinel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shahardalva/sentinel/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white)
![Dependencies](https://img.shields.io/badge/dependencies-stdlib%20only-success)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)
![Status](https://img.shields.io/badge/status-early%20development-orange)

![Sentinel architecture](docs/architecture.svg)

> **Status:** the gateway core in the solid box is implemented and tested. The components in the dashed box are on the roadmap. See [Roadmap](#roadmap).

---

## Contents

- [Why](#why)
- [What works today](#what-works-today)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Request lifecycle](#request-lifecycle)
- [Error responses](#error-responses)
- [Logging](#logging)
- [Performance](#performance)
- [Design decisions](#design-decisions)
- [Project layout](#project-layout)
- [Testing](#testing)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why

Teams are connecting LLMs and agents to internal data and tools faster than they are adding controls around them. Typical gaps:

- Provider API keys are shared across services and developers, with no per-client identity.
- Nothing inspects prompts for leaked secrets or personal data before they leave the network.
- Agents can call any tool exposed to them, and a changed tool description goes unnoticed.
- There is no reliable record of who sent what, and when.

Sentinel addresses these at the network layer, so applications don't need to change beyond their base URL and key.

## What works today

**Security**
- Per-client gateway keys, accepted as `x-api-key` or `Authorization: Bearer`. Keys are compared as SHA-256 digests in constant time.
- The upstream provider key never leaves the gateway. Client credential headers are stripped before the request is forwarded.
- Upstream response headers pass through an allowlist. Organization IDs, cookies and CDN headers are dropped.
- Early rejections (401, 413, 415, 503, 404/405) close the connection with a bounded read deadline, so a client can't hold a connection open by stalling mid-body.
- `anthropic-version` must be a real `YYYY-MM-DD` date. `anthropic-beta` values with control characters are rejected.
- The upstream base URL must be `https` (plain `http` is allowed only for loopback test servers). The gateway refuses to start otherwise.
- Binds to `127.0.0.1` by default. Exposing it on a network is an explicit choice.

**Streaming and timeouts**
- Server-sent events are flushed to the client after every chunk, so tokens arrive as they are generated.
- Separate limits for time to first byte, idle time between chunks, and client write time. A long stream that keeps producing data completes. A stalled one is aborted.
- Graceful shutdown with a 30-second grace period for in-flight requests and streams.

**Resource limits**
- A concurrency cap on requests in flight upstream, returning `503` with `Retry-After` when full.
- The request body is read before a slot is taken, so slow uploads don't hold capacity.
- A declared `Content-Length` over the limit is rejected before any bytes are read. The initial buffer is capped, so a large declared length doesn't reserve memory up front.
- A pooled upstream connection, so requests under load reuse TLS connections.

**Observability**
- One JSON log line per request with the gateway's `request_id`, the provider's `upstream_request_id`, and timings for each stage.
- On failure, the log records the phase where it happened and the cause. Client-side problems log at `WARN`, server and upstream problems at `ERROR`.
- Every response carries an `X-Request-Id` header that matches the log line.

**Engineering**
- Standard library only. No third-party dependencies.
- Provider-agnostic core. The handler knows nothing about Anthropic; provider-specific behavior lives behind a `Provider` interface.
- 44 test functions covering the behavior above, passing under the race detector.

## Quick start

### Requirements

- Go 1.27 or later
- An Anthropic API key (not needed for the mock upstream)
- For the Docker setup: Docker with Compose v2.24 or later, and `make`

### Run with Docker Compose

The compose stack runs the gateway, a mock upstream over HTTPS and Redis (not used by the gateway yet). The gateway is published on `127.0.0.1:8081` only.

```bash
cp .env.example .env   # then fill it in; .env is gitignored
openssl rand -hex 32   # a client key of your own; put it in SENTINEL_API_KEYS
make up                # gateway -> mockupstream over HTTPS
make logs
make down
```

`.env.example` ships with empty keys. Choose your own client key for `SENTINEL_API_KEYS` (the gateway refuses to start without one), and set `ANTHROPIC_API_KEY` only if you will run `make up-real`. Clients then send that key as `x-api-key`.

`make up` points the gateway at `https://mockupstream:9090` and trusts the dev CA from `./certs`. It first runs `make certs`, which writes the certificates (valid for 30 days) when they are missing or expire within 7 days, and recreates the containers so they load the current ones. It replaces `ANTHROPIC_API_KEY` with a dummy, so your real key never reaches the mock. `make up-real` starts the gateway against the real Anthropic API with the keys from `.env`.

The images are built from the `Dockerfile` on a distroless base and run as a non-root user. Without `make`:

```bash
docker compose -f docker-compose.yml -f docker-compose.mock.yml --profile mock up -d --build   # mock
docker compose up -d --build                                                                  # real
```

Run `make` to list the targets with a line on each. `make bench-gateway` load-tests a separate `gateway-bench` service that is fixed to the mock upstream and holds no real key, and refuses to run if that gateway's upstream is anything else, so a load test never reaches the real API.

### Run the gateway

```bash
git clone https://github.com/shahardalva/sentinel.git
cd sentinel

export ANTHROPIC_API_KEY="sk-ant-..."             # the gateway's upstream key
export SENTINEL_API_KEYS="$(openssl rand -hex 32)"  # a client key you choose

go run ./cmd/gateway
```

The gateway logs a `listening` line and serves on `127.0.0.1:8081`.

### Send a request

```bash
curl http://127.0.0.1:8081/v1/messages \
  -H "x-api-key: $SENTINEL_API_KEYS" \
  -H "content-type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "claude-haiku-5-5",
    "max_tokens": 64,
    "messages": [{"role": "user", "content": "Say hello in five words."}]
  }'
```

The client sends its **gateway key**. The real Anthropic key stays inside the gateway.

### Stream a response

Add `"stream": true` to the body and `-N` to curl, so events print as they arrive:

```bash
curl -N http://127.0.0.1:8081/v1/messages \
  -H "x-api-key: $SENTINEL_API_KEYS" \
  -H "content-type: application/json" \
  -d '{"model":"claude-haiku-5-5","max_tokens":64,"stream":true,
       "messages":[{"role":"user","content":"Count from 1 to 10."}]}'
```

### Use it from an SDK

Point the official SDK at the gateway and pass the gateway key:

```python
from anthropic import Anthropic

client = Anthropic(base_url="http://127.0.0.1:8081", api_key="<your client key>")
message = client.messages.create(
    model="claude-haiku-5-5",
    max_tokens=64,
    messages=[{"role": "user", "content": "Hello"}],
)
print(message.content[0].text)
```

## Configuration

All configuration comes from environment variables. Secrets are never read from files in the repository.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `ANTHROPIC_API_KEY` | Yes | | The gateway's own key for the Anthropic API. Clients never see it. |
| `SENTINEL_API_KEYS` | Yes | | Comma-separated client keys. The gateway refuses to start without at least one. |
| `SENTINEL_ADDR` | No | `127.0.0.1:8081` | Listen address. Loopback only by default. |
| `ANTHROPIC_BASE_URL` | No | `https://api.anthropic.com` | Upstream override. Must be `https`, except for a loopback host. |
| `SENTINEL_MAX_CONCURRENT` | No | `128` | Cap on requests in flight upstream. Must be a positive integer. See [ADR 0001](docs/adr/0001-concurrency-cap.md). |
| `SENTINEL_UPSTREAM_CA_FILE` | No | | PEM file of extra CA certificates to trust for the upstream connection, added to the system trust store. For a private or dev CA (see `cmd/devcerts`). Every PEM block must be a valid certificate; the gateway refuses to start if the file is missing, holds none, or holds anything else. The CAs are trusted for whatever host `ANTHROPIC_BASE_URL` names, including the default; the gateway logs a warning in that case, since anyone holding the CA's key could then impersonate the Anthropic API. Does not relax the `https` rule. |

With Docker Compose, the gateway reads these from `.env` (copy `.env.example`; `.env` is gitignored). Compose sets `SENTINEL_ADDR=0.0.0.0:8081` inside the container and publishes the port on `127.0.0.1` only. `make up` also sets `ANTHROPIC_BASE_URL=https://mockupstream:9090`, `SENTINEL_UPSTREAM_CA_FILE=/certs/ca.pem` and a dummy `ANTHROPIC_API_KEY`. The gateway and k6 mount only `certs/ca.pem`; the server key goes only to mockupstream. `BENCH_API_KEY` in `.env` (optional, default `bench`) is the only key the `gateway-bench` service accepts and the one `make bench-gateway` sends.

Built-in limits:

| Limit | Value | Purpose |
| --- | --- | --- |
| Max request body | 32 MiB | Matches the provider's request size limit |
| Concurrent upstream requests | 128 (`SENTINEL_MAX_CONCURRENT`) | Bounds memory and upstream connections |
| Body read timeout | 2 min | Bounds slow uploads |
| Time to first byte | 10 min | Non-streaming requests send nothing until generation finishes |
| Stream idle timeout | 60 s | Aborts a stream that stops producing data |
| Header read timeout | 10 s | Bounds slow request headers |
| Auth read timeout | 5 s | Bounds reads while a request is unauthenticated |
| Shutdown grace period | 30 s | Lets in-flight streams finish |

## Request lifecycle

Each request moves through five stages. Every stage can end the request, and each one is timed.

1. **Auth.** Match the client key in constant time. Strip credential headers.
2. **Intake.** Reject compressed bodies and oversized declared lengths, then read the body under a deadline.
3. **Admission.** Take a concurrency slot, or shed load with `503`.
4. **Provider.** Validate provider headers, attach the gateway's credentials, forward, and filter the response headers.
5. **Stream relay.** Copy the response to the client, flushing every chunk, with idle and write deadlines.

The logged timings map to these stages: `read_body_ms` for intake, `upstream_ttfb_ms` until the provider's response headers arrive, and `stream_ms` for the relay.

## Error responses

Errors use the same JSON shape as the Anthropic API, so existing SDKs parse them unchanged:

```json
{"type": "error", "error": {"type": "authentication_error", "message": "invalid x-api-key"}}
```

| Status | `error.type` | When |
| --- | --- | --- |
| 400 | `invalid_request_error` | Body can't be read, or a provider header is invalid |
| 401 | `authentication_error` | Missing or unknown gateway key |
| 408 | `timeout_error` | Request body not received in time |
| 413 | `request_too_large` | Body larger than the limit, declared or actual |
| 415 | `invalid_request_error` | Request has a `Content-Encoding` |
| 502 | `api_error` | Provider unreachable, or it rejected the gateway's own credentials |
| 503 | `overloaded_error` | Gateway at capacity. Includes `Retry-After` |
| 504 | `timeout_error` | Provider didn't respond in time |

A provider `401` or `403` is reported as `502 upstream authentication failed`, not passed through. Clients never authenticate to the provider, so passing it on would make a revoked gateway key look like a problem with the client's key.

If the client disconnects, the upstream request is cancelled and the log records status `499`. A stream that fails after the `200` has been sent can't change its status; the failure is logged at `ERROR` with `phase: stream`.

## Logging

Logs are JSON on stdout, one line per request:

```json
{
  "time": "2026-10-08T15:46:36.87+03:00",
  "level": "INFO",
  "msg": "request",
  "request_id": "JM4FWMO5DFDCXTXC6U6SFNSX6Z",
  "provider": "anthropic",
  "model": "claude-haiku-5-5",
  "status": 200,
  "latency_ms": 974,
  "read_body_ms": 0,
  "upstream_ttfb_ms": 973,
  "stream_ms": 0,
  "upstream_request_id": "req_011Cfpo4jdYgqjq6yWBZDFEx"
}
```

- `request_id` matches the `X-Request-Id` response header, so a client report can be traced to its log line.
- `upstream_request_id` is the provider's ID for the same request, for support cases with the provider.
- `latency_ms` minus `read_body_ms`, `upstream_ttfb_ms` and `stream_ms` is the time the gateway itself added. In this example it is about 1 ms. That is a single local request, not a benchmark; see [Performance](#performance) for load-test numbers.
- Failed requests add `error` and `phase` (`read_body`, `admit`, `upstream` or `stream`).

## Performance

Load tests against a mock upstream measure the time the gateway itself adds to each request.

- Median gateway overhead is about 1 ms or less at every tested rate up to 1000 req/s.
- p99 spikes moved between runs, which points at the shared test machine rather than a fixed cost in the gateway.
- In one of two runs at 1000 req/s, the concurrency cap shed 0.22% of requests with `503`.

Full numbers are in [bench/RESULTS.md](bench/RESULTS.md). To reproduce them, see [bench/README.md](bench/README.md).

## Design decisions

**Standard library only.** `net/http` covers routing, streaming and per-request deadlines. Fewer dependencies mean a smaller attack surface for a security component.

**A provider-agnostic core.** The handler talks to a `Provider` interface that returns a small `Response` struct. Header filtering, request IDs and version validation live in the Anthropic provider. Adding OpenAI means implementing one `Forward` method; the handler doesn't change.

**Per-chunk deadlines instead of a request deadline.** A single deadline for the whole request would cut off long, healthy streams. Sentinel bounds the time to first byte, the idle time between chunks, and each write to the client instead. The server has no global `ReadTimeout` or `WriteTimeout` for the same reason.

**Read the body before taking a slot.** Otherwise one client could fill every slot with slow uploads and lock everyone else out. Admission happens only once the body is fully in memory.

**Shed load rather than queue.** A queued request still holds a connection and its memory, so queuing only moves the pressure. A fast `503` with `Retry-After` lets clients back off.

**Close the connection on early rejects.** Go's server drains up to 256 KiB of an unread body, with no deadline, before it sends a response. With no global read timeout, a client could send headers, stall, and never receive its `401`. Rejections set `Connection: close` and a short read deadline. A test reproduces the stall over a raw TCP connection.

**Distinguish client errors from upstream errors.** A provider can reject a request with a `RequestError`, which becomes a `400`. Everything else from upstream is a `502` or `504`. The status code tells the client whose problem it is.

## Project layout

```
sentinel/
├── cmd/gateway/              # entry point: config, server, graceful shutdown
├── cmd/mockupstream/         # fake Messages API for load tests, http or https
├── cmd/devcerts/             # writes a dev CA and server certificate to ./certs
├── internal/devcert/         # certificate generation used by devcerts and tests
├── internal/proxy/           # provider-agnostic core
│   ├── auth.go               #   gateway key authentication
│   └── handler.go            #   intake, admission, forwarding, stream relay, logging
├── internal/provider/
│   └── anthropic/            # Anthropic Messages API provider
├── bench/                    # k6 load test and results
├── Dockerfile                # gateway and mockupstream images
├── docker-compose.yml        # local stack; real upstream by default
├── docker-compose.mock.yml   # override: gateway -> mockupstream over HTTPS
├── Makefile                  # dev tasks
├── go.mod
└── LICENSE
```

## Testing

```bash
go test -race ./...
go vet ./...
```

The race detector needs cgo and a C compiler. On Windows, install `gcc` through MSYS2 (`mingw-w64-ucrt-x86_64-gcc`) and add it to `PATH`.

The tests exercise behavior end to end against fake upstream servers, including:

- authentication with missing, wrong and valid keys, and credentials never reaching the upstream or the logs
- a client that sends a partial body without a key, over a raw connection
- streamed events arriving before the stream ends
- a slow first byte, a long active stream, and a stalled stream
- a mid-stream upstream failure and a client disconnect mid-stream
- oversized bodies, declared and actual, and a slow body that must not hold a slot
- the concurrency cap returning `503`
- the upstream rejecting the gateway's key
- response header filtering and `anthropic-version` validation
- an upstream signed by a CA from `SENTINEL_UPSTREAM_CA_FILE` accepted, one from an unknown CA rejected, and a bad CA file stopping startup

## Known limitations

- Only `POST /v1/messages` is proxied. Other Anthropic endpoints return `404`.
- No per-key quotas yet. One valid key can use the whole concurrency budget.
- `anthropic-beta` values are forwarded without an allowlist. Restricting beta features belongs to the planned policy stage.
- The logged `model` field is best effort: it comes from a separate JSON parse and is not authoritative.
- Query strings are not forwarded upstream.
- Streaming has not been load-tested yet.

Each of these is tracked as an issue.

## Roadmap

| Phase | Scope |
| --- | --- |
| **1. Core proxy** | Anthropic provider, auth, streaming, timeouts, resource limits, structured logging. **Done.** |
| **2. Platform** | Docker Compose with Postgres and Redis, CI, per-key rate limits, OpenAI provider, k6 load tests with published p50/p99 overhead |
| **3. Detection** | Secret detection in the gateway, a Python detector service over gRPC for PII and prompt injection, a public evaluation set with precision and recall |
| **4. Agents** | MCP proxy with tool pinning, tool allowlists and scopes per tenant |
| **5. Audit pipeline** | Kafka event stream, workers for LLM-as-judge and alerts, ClickHouse audit log with a hash chain, dashboard |
| **6. Deployment** | Helm chart, Kubernetes with autoscaling and zero-downtime rollouts, AWS with Terraform |

## License

[Apache License 2.0](LICENSE)
