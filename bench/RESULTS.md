# Load test results

Gateway overhead measured with the setup in [README.md](README.md): the same k6 script runs straight at a mock upstream, then through the gateway, and the difference per percentile is the gateway's overhead.

## Setup

| | |
| --- | --- |
| Date | 2026-10-29 |
| Machine | Intel Core i7-7700 @ 3.60 GHz (4 cores, 8 threads), 8 GB RAM |
| OS | Windows 10, Docker Desktop; k6, gateway, Redis and mock all in containers in its one VM |
| Go | 1.27.0 (`golang:1.27.0-alpine` build image) |
| k6 | `grafana/k6:1.3.0`, the compose service |
| Mock | `mockupstream -delay=100ms`, non-streaming, Anthropic route (`BENCH_API` default) |
| Scenarios | 100, 500 and 1000 req/s, constant arrival rate, 30 s each, 5 s apart (`bench/k6/messages.js`) |
| Gateway | `gateway-bench` from `docker-compose.yml`: default concurrency cap 128, all upstreams on the mock with dummy keys, rate limiting through Redis (`redis:7-alpine`, database 1) at `SENTINEL_RATE_LIMIT_RPM=1000000`, fail mode `open` (default) |

### Method

Two pairs, each a `make bench` followed straight away by a `make bench-gateway`:

- `make bench`: k6 → mockupstream over HTTPS with the dev CA. The target runs k6 with `--no-deps`, so the mock must already be up. For these runs only mockupstream (and certs-init, which it depends on) was running; nothing else from the stack.
- `make bench-gateway`: k6 → gateway-bench over plain HTTP on the compose network → mockupstream over HTTPS. The target brings up gateway-bench, Redis and the mock itself (recreated every run, so gateway-bench's counters start at 0), checks the upstream base URLs gateway-bench logged before k6 starts, and checks the run with `cmd/benchverdict` after it.

Every request through gateway-bench makes the Redis rate-limit call, so the overhead includes it. The limit is far above 1000 req/s (60,000 a minute), so no request gets a `429`.

At 100 ms per request, 1000 req/s means about 100 requests in flight, under the gateway's cap of 128. The mock was set to 100 ms instead of its 200 ms default for that reason.

### Verification

`cmd/benchverdict` accepts a gateway run only when gateway-bench's own counters show 0 `fail_open` decisions, `allowed` decisions equal to k6's `http_reqs`, no other outcome, and no `redis not reachable at startup` in its log (see [README.md](README.md#checking-the-run)).

| Run | Verdict |
| --- | --- |
| gateway, first attempt | **rejected**: 2,190 `fail_open`, 44,899 allowed of 47,089 requests |
| gateway, pair 1 | clean: 48,002 requests, 48,002 allowed, 0 `fail_open` |
| gateway, pair 2 | clean: 48,002 requests, 48,002 allowed, 0 `fail_open` |

One of three gateway runs was rejected. That attempt ran with the full `make up` stack (gateway, Postgres, Prometheus, Grafana) in the background, which overloaded the machine: k6 dropped 914 iterations (903 at 500 req/s, 11 at 100 req/s), 117 requests failed at 500 req/s (0.82%), the 500 req/s p99 reached 1.49 s, and gateway-bench's log shows the Redis circuit breaker opening twice. k6 itself passed every threshold (0.82% is under its 1% failure limit), so without the verdict this run would have looked usable. After it, the method changed: only mockupstream runs during `make bench`, and `make bench-gateway` brings up only what it needs. Both accepted pairs were taken that way. The rejected run is not used below.

## Gateway overhead

Overhead = percentile through the gateway minus the same percentile direct to the mock, same pair, same rate. k6 printed p50 (`med`), p90, p95 and p99 for each scenario; those are the ones used. It is the shift in each percentile, not a percentile of per-request overhead.

| Rate | Pair | p50 | p90 | p95 | p99 |
| --- | --- | --- | --- | --- | --- |
| 100 req/s | 1 | +0.76 ms | +0.33 ms | +0.69 ms | +0.98 ms |
| 100 req/s | 2 | +0.96 ms | +0.99 ms | +1.86 ms | +2.61 ms |
| 500 req/s | 1 | +0.88 ms | +0.78 ms | +0.86 ms | +1.21 ms |
| 500 req/s | 2 | +0.81 ms | +0.70 ms | +0.78 ms | +1.16 ms |
| 1000 req/s | 1 | +0.61 ms | +0.82 ms | +0.89 ms | +1.91 ms |
| 1000 req/s | 2 | +0.70 ms | +0.88 ms | +1.02 ms | +2.76 ms |

## Findings

**Median overhead is under 1 ms at every rate**: 0.6 to 1.0 ms in both pairs, with the Redis rate-limit call and one more hop on the Docker network included. The two pairs agree within 0.2 ms at p50.

**p99 overhead is 1 to 3 ms.** 500 req/s was steady (1.2 ms in both pairs); 100 and 1000 req/s varied more between pairs (1.0 vs 2.6 ms, 1.9 vs 2.8 ms). At 100 req/s, p99 rests on about 30 requests per run, so it is noisy.

**One request failed** in pair 2's gateway run, at 1000 req/s: 1 of 48,002, below k6's 1% limit, and outside the percentiles above (they count successful responses only). k6 doesn't print its status. It was fast (the run's fastest request took 8.68 ms, against about 100 ms for any request that reached the mock), and benchverdict counted all 48,002 requests as `allowed`, so it passed the rate limiter and failed later. That fits a `503` from the concurrency cap at admission, which sheds requests when a stall at about 100 in flight piles them past 128: the cap working as designed, not a bug. The gateway's log or metrics would confirm the status; they weren't kept for this run.

## Raw percentiles

`http_req_duration` for successful responses (`expected_response:true`), per scenario, in ms.

| Pair | Path | Rate | p50 | p90 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | direct | 100 | 101.67 | 103.09 | 103.41 | 104.43 | 151.74 |
| 1 | direct | 500 | 100.69 | 101.36 | 101.46 | 101.68 | 119.71 |
| 1 | direct | 1000 | 100.78 | 101.35 | 101.48 | 101.81 | 124.12 |
| 1 | gateway | 100 | 102.43 | 103.42 | 104.10 | 105.41 | 114.10 |
| 1 | gateway | 500 | 101.57 | 102.14 | 102.32 | 102.89 | 117.11 |
| 1 | gateway | 1000 | 101.39 | 102.17 | 102.37 | 103.72 | 124.35 |
| 2 | direct | 100 | 101.41 | 102.49 | 102.98 | 103.72 | 279.22 |
| 2 | direct | 500 | 100.68 | 101.36 | 101.45 | 101.70 | 124.16 |
| 2 | direct | 1000 | 100.77 | 101.36 | 101.50 | 101.96 | 121.49 |
| 2 | gateway | 100 | 102.37 | 103.48 | 104.84 | 106.33 | 118.01 |
| 2 | gateway | 500 | 101.49 | 102.06 | 102.23 | 102.86 | 111.85 |
| 2 | gateway | 1000 | 101.47 | 102.24 | 102.52 | 104.72 | 117.16 |

Requests per run: direct 48,003 and 48,002, gateway 48,002 and 48,002. No dropped iterations in any accepted run or its direct pair.

## Limitations

- k6, the gateway, Redis and the mock share one machine, inside one Docker Desktop VM, and compete for its CPU. The gateway run has more processes to schedule than the direct one, so treat the overhead, the p99 above all, as an upper bound. k6 on a separate machine would isolate the gateway's cost.
- The two paths differ in transport: direct is k6 → mock over HTTPS; through the gateway, k6 → gateway is plain HTTP and gateway → mock is HTTPS. Connections are reused on both, so the TLS handshake is not in the per-request cost.
- The load-start fail-open window ([#55](https://github.com/shahardalva/sentinel/issues/55)) can still reject a run: a stall over 50 ms when k6 starts fails every Redis call in flight at once, which the breaker counts as five failures in a row ([#57](https://github.com/shahardalva/sentinel/issues/57)). Neither accepted run hit it, but a rejected run has to be repeated.
- 1000 req/s runs close to the concurrency cap (about 100 in flight against 128), so a stall can shed a few requests with `503`, as in pair 2.
- Two accepted pairs. Non-streaming only; the streaming path (`STREAM=1`) is not measured yet.
- The mock does almost no work, so this measures the gateway's own cost, not end-to-end latency against a real provider.

## Earlier results (before benchverdict)

The 2026-10-23 runs reported a median overhead of about 1 ms or less, and up to +19.6 ms at p99. They are no longer tabled here: they ran on the host rather than in compose (k6 v2.2.0, gateway logging to a file), and nothing checked their rate-limiting decisions. A later run with the new metrics found 8.6% of requests `fail_open` without any sign in the HTTP statuses, so those figures may be optimistic ([#42](https://github.com/shahardalva/sentinel/issues/42)). Side-by-side tables would invite a comparison the setups don't support. The full tables, findings and the 2026-10-25 caveat are in this file's [git history](https://github.com/shahardalva/sentinel/blob/70c5dc1/bench/RESULTS.md).

## Next

- Decide on the default concurrency cap and document the reasoning; 1000 req/s at 100 ms leaves little headroom.
- Fix the load-start window in the breaker (#57), so stalls stop rejecting runs.
- Measure the streaming path.
- Repeat with k6 on a separate machine.
