# Load test: gateway overhead

Measures how much latency the Sentinel gateway adds on top of its upstream.
The same k6 script runs twice against a mock upstream: once directly, once
through the gateway. The difference between the two runs is the gateway's
overhead.

- `cmd/mockupstream`: a stdlib Go server that imitates Anthropic's
  `POST /v1/messages` and OpenAI's `POST /v1/chat/completions`. It returns a
  fixed response after `-delay` (default 200ms), or a short SSE stream when
  the request has `"stream": true` (`-event-delay` between events, default
  10ms). The OpenAI stream ends with `data: [DONE]`. Listens on `-addr`
  (default `127.0.0.1:9090`).
- `bench/k6/messages.js`: three scenarios at a constant arrival rate of 100,
  500 and 1000 req/s, 30s each, run one after another with a 5s gap. Each
  request is tagged with its scenario (`rate_100`, `rate_500`, `rate_1000`).
  `BENCH_API` picks the protocol.

You need [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) and Go.

## Environment variables for k6

| Variable     | Default                 | Meaning                                    |
|--------------|-------------------------|--------------------------------------------|
| `TARGET_URL` | `http://127.0.0.1:9090` | base URL; the script appends the path      |
| `BENCH_API`  | `anthropic`             | `anthropic`: `POST /v1/messages`, key as `x-api-key`. `openai`: `POST /v1/chat/completions` with an OpenAI-shaped body, key as `Authorization: Bearer` |
| `BENCH_UPSTREAM` | empty               | a gateway's named upstream, as a bare name: `BENCH_API=openai BENCH_UPSTREAM=mockcompat` sends `POST /mockcompat/v1/chat/completions`. The script refuses anything but a name (lowercase letters and digits), since Git Bash rewrites an argument starting with `/` into a Windows path. The mock itself has no prefixed routes, so use it only through a gateway |
| `API_KEY`    | `bench`                 | the key, sent as `BENCH_API` says           |
| `STREAM`     | unset                   | `1` sends `"stream": true`, for either `BENCH_API` |
| `DURATION_S` | `30`                    | seconds per rate; lower it for a smoke run  |

## Run

Start the mock in its own terminal and leave it running for both cases:

```bash
go run ./cmd/mockupstream
```

### (a) k6 straight at the mock

```bash
k6 run bench/k6/messages.js
```

This is the baseline: the mock's fixed delay plus client and loopback cost.

### (b) k6 through the gateway

Start the gateway in another terminal, pointed at the mock. Plain http is
allowed for `ANTHROPIC_BASE_URL` because the host is loopback. The upstream
key is a dummy, since the mock ignores it; the client key is whatever you
then pass to k6 as `API_KEY`.

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:9090 ANTHROPIC_API_KEY=dummy SENTINEL_API_KEYS=bench-key go run ./cmd/gateway
```

In PowerShell:

```powershell
$env:ANTHROPIC_BASE_URL='http://127.0.0.1:9090'; $env:ANTHROPIC_API_KEY='dummy'; $env:SENTINEL_API_KEYS='bench-key'; go run ./cmd/gateway
```

Then run k6 at the gateway (default address `127.0.0.1:8081`):

```bash
TARGET_URL=http://127.0.0.1:8081 API_KEY=bench-key k6 run bench/k6/messages.js
```

In PowerShell, set `$env:TARGET_URL` and `$env:API_KEY` first, the same way.
The gateway logs one JSON line per request to stdout. At high rates, send it
to a file (`> gateway.log`) so terminal output doesn't slow the gateway down.

Repeat both runs with `STREAM=1` to measure the streaming path.

#### The OpenAI route

To measure `POST /v1/chat/completions`, enable the openai upstream on the
gateway, pointed at the same mock (note the `/v1`), and run k6 with
`BENCH_API=openai`:

```bash
SENTINEL_UPSTREAMS=openai OPENAI_BASE_URL=http://127.0.0.1:9090/v1 OPENAI_API_KEY=dummy SENTINEL_API_KEYS=bench-key go run ./cmd/gateway
BENCH_API=openai TARGET_URL=http://127.0.0.1:8081 API_KEY=bench-key k6 run bench/k6/messages.js
```

The direct baseline is `BENCH_API=openai k6 run bench/k6/messages.js`.

### Mock over HTTPS

To measure the gateway with TLS on the upstream leg, as it runs against the
real API, generate a dev CA and server certificate (written to `./certs`,
which git ignores; valid for 30 days):

```bash
go run ./cmd/devcerts
go run ./cmd/mockupstream -tls-cert certs/server.pem -tls-key certs/server-key.pem
```

Then start the gateway with the https URL and the CA file:

```bash
ANTHROPIC_BASE_URL=https://localhost:9090 SENTINEL_UPSTREAM_CA_FILE=certs/ca.pem ANTHROPIC_API_KEY=dummy SENTINEL_API_KEYS=bench-key go run ./cmd/gateway
```

In PowerShell:

```powershell
$env:ANTHROPIC_BASE_URL='https://localhost:9090'; $env:SENTINEL_UPSTREAM_CA_FILE='certs/ca.pem'; $env:ANTHROPIC_API_KEY='dummy'; $env:SENTINEL_API_KEYS='bench-key'; go run ./cmd/gateway
```

Over HTTPS the mock and the gateway negotiate HTTP/2, so requests share a
few multiplexed connections instead of up to 128 HTTP/1.1 connections, and
the TLS handshake is paid about once per connection, not per request. That
is also how the gateway talks to the real API, but it means an HTTPS result
differs from a plain-http one in protocol and connection model, not only in
encryption. For a like-for-like comparison with the plain-http run, add
`-http1` to the mockupstream command to keep the upstream on HTTP/1.1.

`cmd/devcerts` takes `-hosts` (default `localhost,127.0.0.1,mockupstream`),
`-dir` and `-validity`. It never writes the CA's private key; rerun it for a
fresh set.

### With Docker Compose

`make bench` and `make bench-gateway` run the same k6 script in a container
against the compose stack (see the top-level README), over HTTPS with the dev
CA:

```bash
make up              # for make bench: the mock it hits directly
make bench           # k6 straight at mockupstream
make bench-gateway   # k6 through gateway-bench, a gateway fixed to the mock
```

`make bench-gateway` starts its own `gateway-bench` service, which serves
both protocols plus a named upstream, `mockcompat` (protocol `openai`), all
pointed at the mock, and holds no real upstream key. Before k6 starts, it
waits up to about 15 seconds for that gateway to log `listening`, then
checks the `upstream enabled` lines it logged at startup: there must be at
least one, and each must have exactly one `base_url` (the gateway logs only
scheme, host and port), equal to `https://mockupstream:9090`. Otherwise,
or if such a line can't be read, it refuses to run and names the upstream
at fault. It checks what the gateway resolved rather than its environment
variables, since an upstream listed without a base URL variable silently
gets its public default; the matching is in `cmd/benchguard`. `BENCH_API`,
`BENCH_UPSTREAM`, `DURATION_S` and `STREAM` pass through, for example
`make bench-gateway DURATION_S=5`, `make bench-gateway BENCH_API=openai STREAM=1`,
or `make bench-gateway BENCH_API=openai BENCH_UPSTREAM=mockcompat` for the
prefixed route.

`gateway-bench` rate limits through the compose Redis, like a production
gateway, so every request includes the Redis call and the overhead you
measure includes it. Its limit is `SENTINEL_RATE_LIMIT_RPM=1000000`, far above
anything the script sends (1000 req/s is 60,000 a minute), so the bench never
gets a `429`. It uses Redis database 1, apart from the `make up` gateway's
buckets in database 0. The request log's `rate_limit_us` shows the Redis time
on its own. Results from before this change, such as those in
[RESULTS.md](RESULTS.md), have no Redis call, so compare them with care.

### Checking the run

A gateway that can't reach Redis in time still answers: with the default
fail mode, `open`, the request goes ahead, and while the breaker is open it
skips Redis altogether. k6 sees a normal `200`, but the request was cheaper
than a real one, so the run measures less than it claims. After k6
finishes, `make bench-gateway` therefore checks the run with
`cmd/benchverdict` and prints one line either way:

```
bench clean: 47,959 requests, 47,959 allowed, 0 fail_open
```

It rejects the run, with `bench rejected:` and every reason, and exits 1
unless:

1. `sentinel_ratelimit_decisions_total{outcome="fail_open"}` is 0 (absent
   counts as 0);
2. the `allowed` decisions, summed over every series, equal the requests k6
   sent, its `http_reqs` count; every request went through Redis exactly
   once. Any other outcome (`limited`, `fail_closed`, `not_checked`) also
   rejects. A scrape without `sentinel_ratelimit_decisions_total`, or one
   that can't be read, rejects too: no metrics means no proof;
3. gateway-bench's log has its `listening` line and no
   `redis not reachable at startup`.

gateway-bench is recreated for every run, so its counters start at 0 and
cover exactly that run. A clean run exits with k6's own status, so a failed
`http_req_failed` threshold still fails the target.

How the numbers get out, without publishing a port:

- k6's request count comes from its `--summary-export` JSON, set through
  `K6_SUMMARY_EXPORT`, not from its text summary: it is k6's own
  `http_reqs` counter, machine-readable, and it keeps k6's usual summary on
  the terminal (a `handleSummary` in `messages.js` would replace it). It
  counts every request k6 sent, including ones that timed out, so a request
  that never reached the gateway shows up as a mismatch. The file goes into
  the `k6-out` volume of the `k6-gateway` service (`k6` plus that volume, so
  `make bench` is untouched), under a name unique to the run, so a k6 that
  failed to write one can't pass with an older one.
- gateway-bench's `/metrics` is fetched by a second, one-iteration k6 run,
  `bench/k6/gateway-verdict.js`, in the same image on the compose network:
  gateway-bench is distroless (no shell or curl for `docker exec`), its
  metrics port stays unpublished, and no new image is pulled. Prometheus
  isn't used: it scrapes on an interval, so its last sample can miss the
  end of the run. The script prints the summary and the scrape between
  `# benchverdict:` markers, and `cmd/benchverdict` ignores anything else
  k6 prints. The script name reaches compose as `BENCH_K6_SCRIPT` and the
  summary as a bare file name, since Git Bash rewrites an argument that
  starts with `/`.

A rejected run looks like this (the run from the RESULTS.md caveat):

```
bench rejected: 4,058 fail_open: those requests skipped Redis or got no answer from it; 42,823 allowed, but k6 sent 46,881 requests: not every request went through Redis exactly once; gateway-bench logged "redis not reachable at startup" (46,881 requests, 42,823 allowed, 4,058 fail_open)
```

The summary files stay in the `k6-out` volume, a few KB each; `make down`
keeps volumes, and `docker volume rm sentinel_k6-out` clears them.

In compose, mockupstream runs with `-delay 100ms`, the method used in
[RESULTS.md](RESULTS.md): about 100 requests in flight at 1000 req/s, under
the gateway's concurrency cap of 128 (see below). Compare compose runs with
host runs only at the same delay.

## Reading the results

The end-of-test summary has a threshold block per rate, for example:

```
http_req_duration{scenario:rate_500,expected_response:true}
✓ 'p(50)<60000' p(50)=200.57ms
✓ 'p(95)<60000' p(95)=201.33ms
✓ 'p(99)<60000' p(99)=201.87ms
```

The same per-scenario lines also appear under `http_req_duration` in the
metrics list. Latency counts successful responses only. The latency
thresholds are deliberately loose so they always pass; they are there to make
k6 print the percentiles. The `http_req_failed{scenario:...}` threshold
(`rate<0.01`) does fail, and k6 then exits non-zero, so a run with errors is
easy to spot. Don't compare percentiles from a rate whose failure threshold
failed. Check `dropped_iterations` too: a non-zero count means k6 couldn't
keep up the arrival rate, and that rate's numbers are not valid either.

Compute the overhead per rate and per percentile:

```
overhead(rate, pX) = pX(through gateway) - pX(direct to mock)
```

| rate | p50 overhead | p95 overhead | p99 overhead |
|------|--------------|--------------|--------------|
| 100  | gw - mock    | gw - mock    | gw - mock    |
| 500  | gw - mock    | gw - mock    | gw - mock    |
| 1000 | gw - mock    | gw - mock    | gw - mock    |

Only subtract percentiles from the same rate and mode (stream or not). The
result is the shift in each percentile. It is not the percentile of
per-request overhead, which this setup can't observe. For streaming runs,
`http_req_duration` covers the whole stream, so the overhead includes the
gateway's per-chunk cost as well as its time to first byte.

## The gateway's concurrency cap

The gateway allows 128 requests in flight upstream
(`proxy.DefaultMaxConcurrent`) and answers extra ones with a fast 503. In
flight is roughly rate × latency, so at the default 200ms delay:

- 100 req/s → ~20 in flight
- 500 req/s → ~100 in flight (streaming, at ~280ms, is ~140: over the cap)
- 1000 req/s → ~200 in flight: over the cap

So by default the 1000 req/s run through the gateway fails its
`http_req_failed` threshold with a large share of 503s. That is the cap doing
its job, not overhead. To measure latency at 1000 req/s, lower the mock's
delay so rate × latency stays under 128, for example
`go run ./cmd/mockupstream -delay 100ms`. Use the same delay for both the
direct and the gateway run, or the subtraction is meaningless.

## Caveats

- Results depend on the machine. Compare runs from the same machine, with
  the same settings, close together in time.
- k6, the gateway and the mock all run on one machine and compete for CPU.
  At high rates, k6 itself is often the biggest CPU user, and that inflates
  the gateway run more than the direct run, since the gateway run has one more
  process to schedule. Treat the overhead at 1000 req/s as an upper bound.
  For cleaner numbers, run k6 on a separate machine. The gateway then needs
  `SENTINEL_ADDR` set to a non-loopback address.
- Use a few runs, not one. p99 over 30s at 100 req/s rests on about 30
  samples, so it is noisy.
