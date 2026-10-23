# ADR 0001: Configurable concurrency cap

- Status: accepted
- Date: 2026-10-23

## Context

The gateway caps requests in flight upstream. Until now the cap was a fixed 128, and the Anthropic provider's idle connection pool was a second constant of 128 kept in step by hand.

The load test in [bench/RESULTS.md](../../bench/RESULTS.md) ran at 1000 req/s against a mock upstream with a 100 ms delay, so about 100 requests were in flight on average. In one of two runs, the gateway returned `503` to 67 requests (0.22%). The gateway log confirms exactly 67 `503` responses, matching what k6 reported. A short stall on the shared test machine let in-flight requests pile up past 128, and the gateway shed them. The cap worked as designed, but 128 left little headroom at that rate and latency, and operators had no way to change it.

## Decision

- The cap is set by `SENTINEL_MAX_CONCURRENT`. The default stays at 128.
- The value must be a positive integer. The gateway refuses to start on anything else, instead of falling back to the default without saying so.
- The provider's idle connection pool is sized to the same value, so the two can't drift apart.
- At the cap, the gateway still sheds load with a fast `503` and `Retry-After` rather than queueing. A queued request still holds a connection and its memory, so a queue only moves the pressure.

## Sizing

By Little's law, requests in flight ≈ request rate × upstream latency. Set the cap to that, plus headroom for bursts and stalls:

```
cap ≈ peak req/s × typical upstream latency (s) × 1.5 to 2
```

For example, 1000 req/s at 100 ms is about 100 in flight, so a cap of 150 to 200. Real LLM calls take seconds, not 100 ms: 20 req/s at 5 s is also about 100 in flight.

## Trade-off

The cap also bounds memory. Each admitted request holds its full body in memory, so the worst case is cap × max body size: 128 × 32 MiB = 4 GiB with the defaults. Raising the cap raises that bound in step. A higher cap sheds fewer requests during bursts, but needs a machine that can hold the worst case or smaller request bodies.
