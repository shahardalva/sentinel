# Load test: gateway overhead

Measures how much latency the Sentinel gateway adds on top of its upstream.
The same k6 script runs twice against a mock Anthropic upstream: once
directly, once through the gateway. The difference between the two runs is
the gateway's overhead.

- `cmd/mockupstream`: a stdlib Go server that imitates `POST /v1/messages`.
  It returns a fixed message after `-delay` (default 200ms), or a short SSE
  stream when the request has `"stream": true` (`-event-delay` between
  events, default 10ms). Listens on `-addr` (default `127.0.0.1:9090`).
- `bench/k6/messages.js`: three scenarios at a constant arrival rate of 100,
  500 and 1000 req/s, 30s each, run one after another with a 5s gap. Each
  request is tagged with its scenario (`rate_100`, `rate_500`, `rate_1000`).

You need [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) and Go.

## Environment variables for k6

| Variable     | Default                 | Meaning                                    |
|--------------|-------------------------|--------------------------------------------|
| `TARGET_URL` | `http://127.0.0.1:9090` | base URL; the script appends `/v1/messages` |
| `API_KEY`    | `bench`                 | sent as `x-api-key`                         |
| `STREAM`     | unset                   | `1` sends `"stream": true`                  |
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
