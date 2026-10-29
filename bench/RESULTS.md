# Load test results

Gateway overhead measured with the setup in [README.md](README.md): the same k6 script runs straight at a mock upstream, then through the gateway, and the difference per percentile is the gateway's overhead.

## Setup

| | |
| --- | --- |
| Date | 2026-10-23 |
| Machine | Intel Core i7-7700 @ 3.60 GHz (4 cores, 8 threads), 8 GB RAM |
| OS | Windows 10 (k6, gateway and mock on the same machine) |
| Go | 1.27 |
| k6 | v2.2.0 |
| Mock delay | 100 ms per request, non-streaming |
| Scenarios | 100, 500 and 1000 req/s, constant arrival rate, 30 s each |
| Gateway | default settings, concurrency cap 128, logs written to a file |

The mock delay was lowered from the default 200 ms to 100 ms, so that 1000 req/s (about 100 requests in flight) stays under the gateway's concurrency cap of 128.

## Gateway overhead

Overhead = percentile through the gateway minus the same percentile direct to the mock, same run pair.

### Caveat: fail-open windows

A `make bench-gateway` run on 2026-10-25, watched with the new metrics, showed that a bench can run with rate limiting partly switched off. gateway-bench logged `redis not reachable at startup`, and the Redis circuit breaker opened four times during the 100 and 500 req/s stages. 4,058 of about 47,000 requests (8.6%) were `fail_open` in `sentinel_ratelimit_decisions_total`. The log agrees: its fail-open `WARN` lines plus their `suppressed` counts add up to 4,054, and the last batch of a flood is never logged. HTTP statuses alone wouldn't show it: a fail-open request is answered like any other.

While the breaker is open, a fail-open request skips the Redis call entirely, so it is cheaper than a normal one; only the few failures that open the breaker wait out the 50 ms timeout. The overhead figures in this file may therefore be optimistic, especially at 100 req/s; the earlier runs had no metrics that would have shown fail-open requests. Detecting and rejecting such runs is tracked in [#42, "Bench runs can include fail-open windows; detect and reject them"](https://github.com/shahardalva/sentinel/issues/42).

### Run 1

| Rate | p50 | p95 | p99 | Failed |
| --- | --- | --- | --- | --- |
| 100 req/s | +0.74 ms | +1.11 ms | +1.01 ms | 0 |
| 500 req/s | -0.04 ms | -0.11 ms | +19.58 ms | 0 |
| 1000 req/s | 0.00 ms | -0.03 ms | +0.26 ms | 0 |

### Run 2

| Rate | p50 | p95 | p99 | Failed |
| --- | --- | --- | --- | --- |
| 100 req/s | +1.01 ms | +1.01 ms | +0.97 ms | 0 |
| 500 req/s | +0.03 ms | +0.04 ms | +0.27 ms | 0 |
| 1000 req/s | +0.02 ms | +0.27 ms | +6.35 ms | 67 (0.22%), all 503 |

Small negative values are measurement noise, not a gateway that speeds requests up.

## Findings

**Median overhead is about 1 ms or less.** At p50 the gateway added 0.7 to 1.0 ms at 100 req/s and effectively nothing at 500 and 1000 req/s, consistently across both runs. The higher figure at the lowest rate is consistent with CPU power management between requests, but that has not been verified.

**Tail spikes move between runs.** Run 1 had a p99 spike at 500 req/s (+19.6 ms), run 2 at 1000 req/s (+6.4 ms, one request at 237 ms). A spike that changes rate from run to run points at the shared test machine (Windows scheduling, other processes, k6 competing for CPU) rather than a fixed cost in the gateway. With k6 on the same machine, treat the p99 figures as an upper bound.

**The concurrency cap leaves little headroom at 1000 req/s.** With about 100 requests in flight on average and a cap of 128, a short machine stall lets in-flight requests pile up past the cap, and the gateway sheds them with a fast 503. The gateway log confirms exactly 67 responses with status 503, matching the 67 failed requests k6 reported. This is the cap working as designed; the open question is whether 128 is the right default for this rate and latency.

### Metrics cross-check, 2026-10-25

A smoke test of the metrics branch (`make up`, then k6 at 1000 req/s through the gateway, with Prometheus and Grafana running on the same machine) was not a new measurement, but the counts agreed exactly: k6, Prometheus (`sentinel_requests_total`) and the gateway log each saw 47,959 requests, 47,811 answered `2xx` and 148 shed with `503` at admission (`phase="admit"`). Shedding at this rate matches run 2 above, with two more services competing for the machine.

## Raw percentiles

`http_req_duration` for successful responses, in ms.

| Run | Path | Rate | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | direct | 100 | 100.77 | 102.13 | 102.83 | 105.53 |
| 1 | direct | 500 | 100.56 | 101.50 | 102.49 | 139.86 |
| 1 | direct | 1000 | 100.37 | 100.88 | 101.54 | 123.90 |
| 1 | gateway | 100 | 101.51 | 103.24 | 103.84 | 106.42 |
| 1 | gateway | 500 | 100.52 | 101.39 | 122.07 | 196.71 |
| 1 | gateway | 1000 | 100.37 | 100.85 | 101.80 | 115.69 |
| 2 | direct | 100 | 100.93 | 102.26 | 102.79 | 119.37 |
| 2 | direct | 500 | 100.56 | 101.45 | 102.12 | 121.25 |
| 2 | direct | 1000 | 100.36 | 100.73 | 101.06 | 113.67 |
| 2 | gateway | 100 | 101.94 | 103.27 | 103.76 | 119.39 |
| 2 | gateway | 500 | 100.59 | 101.49 | 102.39 | 120.08 |
| 2 | gateway | 1000 | 100.38 | 101.00 | 107.41 | 236.59 |

## Limitations

- Client, gateway and mock share one machine. Running k6 on a separate machine would isolate the gateway's cost, especially at p99.
- Two runs per configuration. p99 at 100 req/s rests on about 30 samples per run and is noisy.
- Non-streaming requests only. The streaming path (`STREAM=1`) has not been measured yet.
- The mock does almost no work, so this measures the gateway's own cost, not end-to-end latency against a real provider.

## Next

- Decide on the default concurrency cap and document the reasoning.
- Measure the streaming path.
- Repeat with k6 on a separate machine.
