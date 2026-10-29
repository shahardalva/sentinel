# ADR 0002: Per-key rate limiting in Redis

- Status: accepted
- Date: 2026-10-23

## Context

Until now one valid client key could use the gateway's whole concurrency budget (ADR 0001), and with it the whole upstream account. The concurrency cap protects the gateway, not other clients: one runaway script could take every slot and get the others a `503`.

The limit has to hold per client key, and keep holding when more than one gateway instance runs behind a load balancer. That rules out a counter in each process's memory: with N instances, a client would get N times its limit, depending on where its requests land.

## Decision

- Each client key gets a **token bucket** in Redis. The bucket holds `SENTINEL_RATE_LIMIT_RPM` tokens (default 60) and refills at that many per minute. Each request takes one token. An empty bucket gets `429 rate_limit_error` with `Retry-After` set to the seconds until the next token, rounded up.
- The check runs **after auth and before the body is read**. A rejected request costs one Redis round trip and no upload, and closes the connection like the other early rejections.
- The bucket is read, refilled, decremented and written by **one Lua script**, which Redis runs atomically. Two requests for the same key, on the same instance or on different ones, can't both spend the last token. The script uses Redis's own clock (`TIME`), so clock skew between gateway instances doesn't change the refill. If that clock steps back, for example after a failover to a replica whose clock is behind, the script counts it as no time passing and refills from the new clock, instead of refilling nothing until the old timestamp comes around again.
- The Redis key is `sentinel:ratelimit:` followed by the **hex SHA-256 digest of the client key**, never the key itself. Anyone who can read Redis sees only digests. Client keys should be long and random (`openssl rand -hex 32`), since a short key's digest can be brute-forced. Each key expires once its bucket would be full again, so idle clients leave nothing behind.
- Every Redis call has a **50 ms timeout**, enforced by the call's context, which covers the pool wait, any wait for a new connection, and the reply. A slow Redis adds at most that to a request.
- **New connections are dialed outside that budget.** go-redis dials on a goroutine of its own, under the pool's `DialTimeout` (2 s) per attempt, not under the call's context: a call that needs a new connection still gives up at 50 ms, and the dial carries on to fill the pool. So at startup, after its ping, the gateway sets up 16 connections (`ratelimit.WarmConns`, at most the pool size) and sends a `PING` on each, so they are dialed and past the handshake (`HELLO`, `AUTH`, `SELECT`) before the first request: at 1000 req/s and a 1 to 5 ms round trip, 1 to 5 calls are in flight, so 16 leaves room for bursts. The warm-up's reads get 2 s, not 50 ms; it takes at most 5 s and stops on a signal. It logs `redis pool warm` at `INFO` with the count, or `redis pool warm-up incomplete` at `WARN` with how many are ready, and starts either way: the fail mode covers the rest. A failed startup ping skips it. The pool keeps those connections while idle (go-redis closes one after 30 idle minutes) but no minimum: go-redis's `MinIdleConns` would dial as many again during the warm-up, and refill after every use. Until 2026-10-29 the dial timeout was the call's 50 ms too, so a cold pool failed its first calls at load start, which opened the breaker.
- **Maintenance notifications are off.** They are a Redis Cloud and Enterprise feature for planned maintenance (relaxed timeouts, handing a connection to a new endpoint). In go-redis's default auto mode every new connection asks for them, and on a Redis without them the first refusal switches the mode off in options that connections set up at once share under separate locks: a data race in the warm-up. Off, nothing the gateway uses changes; on Redis Cloud or Enterprise we lose that graceful handoff, and maintenance looks like dropped connections, whose calls fail open without opening the breaker (below).
- The Redis connection pool is sized from `SENTINEL_MAX_CONCURRENT`, not go-redis's default of 10 per CPU. The check runs before the concurrency cap, so more requests than that can still check at once; those that find every connection busy for the whole 50 ms get the fail mode with `ErrBusy`. That is load on the gateway, not a Redis failure, so it doesn't count toward the circuit breaker below. Neither does a call left with less than half the timeout after waiting for a connection. Otherwise a burst from one client could open the breaker while Redis is healthy, and in fail-open mode switch the limit off for everyone.
- Without `SENTINEL_REDIS_URL` rate limiting is off, and the gateway says so at startup.

### Why Redis

Redis is the shared state the limit needs, and it's already on the roadmap for the platform phase. One round trip is a fraction of a millisecond on a local network, which fits a hot path that has to stay fast and measurable. The duration is logged as `rate_limit_us` on every request. Postgres could hold the same state, but a row update per request costs more, and the gateway doesn't use Postgres yet.

### Why go-redis as the first dependency

Until now the gateway used only the standard library, to keep the attack surface small. A Redis client is the point where that stops paying off. Writing RESP parsing, connection pooling, reconnects, `EVALSHA` with a fallback to `EVAL`, and TLS for `rediss://` by hand is real security-relevant code that would need its own tests and upkeep. `github.com/redis/go-redis/v9` is the client maintained by Redis itself and is widely used. It honors context deadlines when `ContextTimeoutEnabled` is set, which is how the 50 ms bound is enforced. It brings three indirect modules: `xxhash`, `go.uber.org/atomic` and `golang.org/x/sys`.

Tests use `github.com/alicebob/miniredis/v2`, an in-process Redis that runs Lua and lets a test set the server clock. That makes refill tests exact and keeps CI free of a Redis service. It is a test-only dependency and is not compiled into the gateway. miniredis runs Lua with gopher-lua, not Redis's own interpreter, so a second set of tests runs the script on a real Redis when `SENTINEL_TEST_REDIS_URL` is set. CI runs them against a `redis:7-alpine` service, and `make test-redis` runs them locally in a throwaway container.

### Why a token bucket

- **Fixed window** (`INCR` plus `EXPIRE` per minute) is simpler, but allows twice the limit around a window boundary, and every client's budget resets at the same instant.
- **Sliding log** (a sorted set of timestamps) is exact, but stores one entry per request, so memory grows with the limit.
- **Token bucket** stores two numbers per key, allows a burst up to the bucket size, and then holds the client to the steady rate. Its next-token time also gives an exact `Retry-After`. The burst equals one minute's worth, so a client that has been idle for a minute can send its full allowance at once. That matches how LLM clients behave: idle, then a batch of calls.

## Fail-open default and its trade-off

When Redis can't answer, because it's down, unreachable or past the 50 ms timeout, the gateway has two options:

- **Fail open** (default, `SENTINEL_RATE_LIMIT_FAIL_MODE=open`): let the request through, without a limit. The gateway logs a `WARN` at most once every 10 seconds, with a count of the failures it didn't log, so an outage can't flood the log.
- **Fail closed** (`closed`): answer `503 api_error` with `Retry-After: 1`. The request line is logged at `ERROR` with `phase: rate_limit`.

A circuit breaker sits in front of Redis. After 5 failed calls in a row it opens: for the next 5 seconds Redis is not called at all, and the fail mode applies at once, with no 50 ms wait. Then one request probes Redis while the others keep skipping it. If the probe succeeds the breaker closes, and if it fails the breaker stays open for another 5 seconds. Only the probe decides: calls already in flight when it opened report on Redis as it was then, so a late success doesn't close it. Otherwise a Redis that answers some calls in time and not others would flip the breaker many times a second. Opening is logged at `WARN` and closing at `INFO`. Neither does a call that failed only because a pooled connection had been closed under it (`EOF`, connection reset or aborted, broken pipe, a closed connection; never a failed dial): when Redis restarts or fails over, every open connection is dead, and over TLS (or on Windows) go-redis can't check an idle one before using it. go-redis drops it and the next call gets another or a new one, so counting these would open the breaker for a whole cooldown with Redis healthy again; such a call still gets the fail mode. For the same reason go-redis's retry backoff is off: a call on a dead connection is retried on another one at once, instead of sleeping under its 50 ms until it times out, which does count. Calls the client abandoned don't count as failures, and the request is logged as `499` at `WARN`, with no `503` and no fail-open warning. Without the breaker, a dead Redis would add the full timeout to every request, and every request would add load to a Redis that is trying to recover. go-redis's own log output, a line per failed dial, goes to the gateway's logger at `DEBUG`, so an outage can't flood stderr.

Open is the default because the rate limit protects fair use and cost, not confidentiality. Authentication, header stripping and the concurrency cap don't depend on Redis and keep working. With fail closed, a Redis outage becomes a full outage of the gateway, and Redis becomes as critical as the gateway itself.

The cost is that **during a Redis outage there is no per-key limit**. A client that can take Redis down, or that happens to be sending during an outage, is bounded only by the concurrency cap. Deployments where spend control matters more than availability, for example where keys are handed to untrusted parties, should set `closed` and run Redis with replication. Either way, the warning or the `503`s make the outage visible, and the startup log warns if Redis isn't reachable when the gateway starts.

## Consequences

- One more service to run in production. Compose already includes it.
- Requests cost one Redis round trip each, logged as `rate_limit_us`.
- The limit counts requests, not tokens. A limit on input and output tokens per key needs the response's usage numbers and belongs with the planned policy stage.
- All keys share one limit. Per-key limits need a key store with metadata, which comes with the Postgres-backed tenant model.
