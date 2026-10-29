# Development notes

Newest entries first. Short: what was done, what was decided, what's next.

## 2026-10-29: warm Redis pool, dials outside the 50 ms (#55)

### Done (test first, then a mutation check per part)
- Evidence (#55): a bench-gateway run at 100 req/s, startup ping OK; the
  first 5 rate-limit calls failed with "context deadline exceeded"
  within ~50 ms of the first request, the breaker opened for 5 s, and
  ~490 requests failed open. The pool had one connection, the ping's;
  every other one had to be dialed inside the 50 ms.
- ratelimit.NewClient: DialTimeout is PoolDialTimeout (2 s), no longer the
  call's timeout; MinIdleConns 16, at most the pool size. Read, write and
  pool timeouts and the 50 ms call context are unchanged.
- NewClient also disables go-redis maintenance notifications (see
  Learned: a data race in go-redis otherwise).
- ratelimit.WarmUp holds MinIdleConns connections and PINGs each, then
  returns them. cmd/gateway runs it in logRateLimit after a successful
  ping, bounded by 5 s: INFO "redis pool warm" ready=N, or WARN "redis
  pool warm-up incomplete" ready, want, fail_mode, error; startup goes on.
- Tests: coldRedis, a TLS proxy to miniredis whose handshakes wait
  300 ms (a local TCP connect is instant, so the handshake is the part of
  a dial a test can slow down, and the dial timeout covers it). 16
  concurrent calls at the real 50 ms after WarmUp: before the fix 16 of
  16 failed and the breaker opened, the bench in miniature; after, 0.
  Gateway: the INFO line with ready=16 on miniredis, and the WARN with
  ready=4 want=16 against a proxy serving only the first 4 connections.
- Mutations, each failing a test: no MinIdleConns (both packages);
  DialTimeout back to 50 ms (the burst: WarmUp 0, 16 of 16 calls fail,
  breaker open); no warm-up call at startup (both gateway tests, on the
  log lines; the connection count alone doesn't catch it, since
  go-redis's own background dials get there anyway).

### Decided
- 16 idle: 1000 req/s x 1-5 ms per call is 1-5 in flight; 16 covers a
  burst at load start for 16 sockets per gateway. 2 s per dial: a cold
  DNS lookup, TCP and TLS on a slow network, or a retried DNS query; the
  dial is never on a request's clock, and a dead Redis holds a dial
  goroutine at most 2 s per attempt (5 attempts). 5 s for the warm-up:
  two dial timeouts and some.
- Warm-up skipped when the startup ping fails: it could only wait out
  its 5 s. Not interruptible by a signal, like the ping (bounded, and
  before any listener).
- synctest doesn't fit: the dial runs on real sockets (go-redis's own
  dialer), which aren't durably blocking. The tests rely on margins
  instead: 300 ms against 50 ms and 2 s, no sleep-to-wait.

### Learned
- The breaker turned 5 failures into ~490 fail-opens: 5 calls in a row
  failing on a cold pool opened it, and for its 5 s cooldown every request
  (~100 req/s) got the fail mode with Redis healthy. A ~100x amplifier of
  a startup transient; breaker behavior unchanged here, for the upstream
  circuit breaker work next.
- go-redis v9.23.0: a Get that finds no idle connection queues a dial on
  its own goroutine with context.Background(), DialTimeout per attempt,
  up to 5 attempts (100 ms backoff). The caller waits on its own context
  and gives up at 50 ms; the dial carries on and the connection goes to
  the next waiter or the idle list. So DialTimeout, not the call, bounded
  the dial, and at 50 ms it killed slow dials outright. The default dialer
  is net.Dialer{Timeout: DialTimeout} (DNS + TCP); for rediss://,
  tls.DialWithDialer, where Timeout covers TLS too and the context is
  ignored.
- MinIdleConns is filled in the background: NewConnPool starts one
  goroutine per missing connection (Background context, DialTimeout per
  attempt) and refills after removals. Nothing waits for them, and they
  are dialed but not initialized: HELLO/AUTH/SELECT run on the first Get,
  under that caller's context. Hence WarmUp's PINGs. ReadTimeout (50 ms)
  still caps each read during warm-up, fine for a Redis that answers.
- go-redis checks idle connections for a closed peer on Get only on
  Unix (conn_check_dummy.go elsewhere). On Windows, after a Redis
  restart, the warm pool's dead connections are found by failed calls
  plus retry backoff, which can use up 100 ms: TestBreaker_
  FewerFailuresStayClosed now creates its client with Redis already down
  and pings before the "Redis back" call. The gateway runs on Linux.
- go-redis data race (-race caught it in WarmUp): each client.Conn()
  shares the client's *Options but has its own optLock, and in the
  default maint-notifications "auto" mode a Redis that refuses CLIENT
  MAINT_NOTIFICATIONS (miniredis, OSS Redis) makes initConn write
  Options.MaintNotificationsConfig.Mode = disabled. Concurrent handshakes
  on several Conns race on it. NewClient disables maint notifications (a
  Redis Cloud/Enterprise feature; also one handshake command less per
  connection), and a test checks it.
- TestRouteClientLogs used a call's failed dial to get a go-redis log
  line; that dial now outlasts the test, so it uses its own client with a
  50 ms dial and one attempt.

### Next
- Re-run make bench-gateway (needs docker, for the user): expect
  "redis pool warm" ready=16 in gateway-bench's log and a clean verdict.
- Upstream circuit breaker; revisit the Redis breaker's amplification
  then (e.g. not counting failures while the pool is still dialing).

## 2026-10-29: make bench-gateway rejects unclean runs (#42)

### Done (test first, then a mutation check per rule)
- cmd/benchverdict, run after k6: rejects unless gateway-bench's own
  counters show 0 fail_open (absent is 0), allowed summed over all series
  equal to k6's http_reqs, no other outcome, and its log has "listening"
  and no "redis not reachable at startup". No decisions metric, or output
  it can't read, rejects: no metrics, no proof. One line either way,
  "bench clean: N requests, N allowed, 0 fail_open" or "bench rejected:"
  with every reason; exit 1 on a rejection, else k6's own status.
- Prometheus text parsed with expfmt (prometheus/common, already in the
  module graph, now direct), so label order and escaping don't matter.
  Fixtures in cmd/benchverdict/testdata from a real scrape, plus a test
  on a live internal/telemetry scrape, so a renamed metric fails a test,
  not a bench.
- Compose: k6-gateway extends k6 with a k6-out volume on /home/k6 and the
  script from BENCH_K6_SCRIPT; make bench and the k6 service are
  unchanged. bench/k6/gateway-verdict.js prints the summary and
  gateway-bench:9464/metrics between "# benchverdict:" markers.

### Decided
- k6's count from K6_SUMMARY_EXPORT: k6's own http_reqs, machine-readable,
  and the terminal summary stays (handleSummary in messages.js would
  replace it). A file name unique per run, so a k6 that wrote none can't
  pass with an old one.
- Metrics fetched by a second k6 run on the compose network: no published
  port, no new image, and gateway-bench is distroless (nothing to docker
  exec). Not Prometheus: its last scrape can miss the end of the run.
- The startup-log rule is checked after k6 too, not before it, as asked;
  failing fast before the load would save the 105 s of a run that will be
  rejected anyway. Revisit if it rejects often.

### Learned (hypothesis, not verified: needs a bench run)
- "redis not reachable at startup" despite depends_on redis
  service_healthy: the startup Ping has a 1 s context, but
  ratelimit.NewClient sets DialTimeout and ReadTimeout to
  DefaultTimeout, 50 ms, and net.Dialer's Timeout includes the DNS
  lookup. So the first connection of a fresh container gets 50 ms for
  "redis" through Docker's embedded DNS (127.0.0.11), the TCP connect,
  HELLO and SELECT 1, on a Docker Desktop VM busy right after --build
  and recreating its dependencies. "i/o timeout" is that dialer's
  timeout; a Redis that wasn't up would say "connection refused", and
  the healthcheck had already passed redis-cli ping. DB 1 adds only the
  SELECT round trip. The breaker trips at 100 and 500 req/s fit the same
  budget: the pool (up to 128) dials new connections as concurrency
  rises, each with a DNS lookup under 50 ms, with k6 on the same CPU.
- To confirm: the WARN's error field says which step timed out ("lookup
  redis on 127.0.0.11:53" is DNS, "dial tcp <ip>:6379" the connect,
  "read tcp" a reply), and the breaker's open lines should cluster at
  stage starts (0 s, 35 s, 70 s). No startup change until then.

### Next
- Re-run the RESULTS.md baselines under the check (#42 part 3).
- If the hypothesis holds: a warm-up stage, or a startup Ping that gets
  more than 50 ms to dial (#42 part 1).

## 2026-10-29: v0.6.0 polish (review of PR #51)

### Done (behavioral items test first, then a mutation check)
- Admin bind WARN: a SENTINEL_ADMIN_ADDR host other than localhost or a
  loopback IP (empty, 0.0.0.0, ::, any other name) logs one WARN: plain
  HTTP, so the token and new keys cross the network. Expected under
  compose (0.0.0.0 in the container, published on 127.0.0.1 only).
- TestServe_DatabaseURLNotLeaked: DNS failure and connection refused in
  key=value form, which reach the connect path (the old bad port and bad
  sslmode cases fail at parsing). Wrong user and password against
  SENTINEL_TEST_POSTGRES_URL, gated on it.
- SENTINEL_API_KEYS of only commas or spaces: "has no keys", not "is not
  set".
- Revocation latency docs: refresh plus up to two fetch timeouts. The
  fetch timeout is min(refresh/2, 10 s), so at most 2 x refresh, 50 s with
  the defaults (not 1.5 x / 45 s: the default caps it at 10 s, and the
  read and the swap can each be a fetch timeout apart).
- Grafana: "Key set stale" state timeline (0 fresh green, 1 stale red)
  beside the other key panels, now four across.

### Skipped
- An extra SchemaVersion call after Migrate: startup only, two queries;
  not worth folding into Migrate's result.

### Next
- alert on sentinel_auth_keys_stale; needs alerting infra (none yet).

## 2026-10-29: v0.6.0 release blockers (review of PR #51)

### Done (each test first, then the fix, then a mutation check)
- SENTINEL_KEYS_MAX_STALE (default 10m, 0 never): past it since the last
  good fetch, keys.Refreshing.Keys() returns proxy.UnavailableKeySet()
  and proxy.Auth answers every request 503 (api_error), not 401. ERROR
  once on entering, INFO once on recovering, from a request or a refresh,
  whichever sees it first. Gauge sentinel_auth_keys_stale through a new
  Recorder.RecordKeysStale. Config: negative, or under three times
  SENTINEL_KEYS_REFRESH (the default too), stops startup; without a DB
  only a WARN. Compose passes it like SENTINEL_KEYS_REFRESH.
- Admin shutdown: the admin server stops accepting when shutdown starts,
  alongside the API drain; after its 5 s grace and Close, admin.API.Drain
  waits up to 6 s (StoreTimeout + 1 s) for handlers still in a store
  call, so a committed change always gets its audit line before the pool
  closes. A handler past that: WARN, pool left open (stop(closeStore)).
- Admin in-flight cap: admin.MaxInFlight = store.MaxConns / 2 = 4, 503
  with Retry-After: 1 at once past it, after the token check.
  store.MaxConns = 8 set on the pool config, overriding pool_max_conns.
- startupInterrupted: exit 0 only when the error is Canceled or
  DeadlineExceeded and the signal ctx is done; ErrSchemaTooNew during a
  signal is an error exit.

### Decided
- Stale travels in the KeySet (UnavailableKeySet), not a second
  KeySource method or an error return: one call per request, one answer,
  no change to Static or the fakes' Keys.
- The max-stale floor is 3 x refresh (first 2 x, raised after review):
  the fetch timeout is <= refresh/2, so after one failed refresh the next
  success lands by 3 x refresh; with 2 x, a single failed refresh puts
  every request on a 503. keys.NewRefreshing enforces 2 x (Interval +
  FetchTimeout), the same bound for any fetch timeout.
- Admin cap is a constant derived from the pool size, not configurable.
- 503s for stale keys aren't auth rejections; no per-request counter.

### After review (go-reviewer), tests first
- S2: the stale log and gauge could stick at "stale" while fresh keys
  were served: a request deciding "stale" under staleMu, paused before
  recording it, while a refresh succeeded and its lock-free check saw
  nothing to change. store() and refresh() now always recheck under
  staleMu (recheckStale); requests keep the lock-free path. Test drives
  the interleaving with two hooks (export_test.go), no sleeps.
- S3: TestServe_StaleKeysAre503 reads the sentinel_auth_keys_stale line
  itself (1 while stale, 0 after recovery); the old check matched the
  key-count gauge's "} 1".
- N1: a Drain after one that returned nil returns nil at once; Drain
  calls are serialized.
- N5: floor raised to 3 x refresh (above).
- TestStartupInterrupted covers a real *store.Error from store.Open on a
  cancelled ctx, wrapped by startupError.
- S1 (docs only): the "4 connections always free" guarantee holds only
  while Postgres answers; a timed-out admin call frees its slot at once
  but pgx keeps its connection up to 15 s to clean up, and Close waits
  for it, so shutdown can run ~15 s longer.

### Accepted
- N2: the admin slot is taken before the body is read, so a holder of
  the token sending slow bodies can keep slots for up to the 10 s read
  timeout. Token required; the refresh is unaffected.
- N3: an own-deadline timeout (DeadlineExceeded) can read as "startup
  interrupted" and exit 0 if a signal arrives between the step returning
  and the check. The window is tiny and the exit code barely matters
  there.
- N4: the stale 503 has no Retry-After and sends Connection: close, so
  during a stale period every request reconnects. The gateway can't know
  when the database is back; SDKs back off on 503.

### Next
- A separate small pool for the key refresh (S1), so admin timeouts
  against an unresponsive Postgres can never hold its connection.
- Grafana panel (done in the polish entry above) and an alert rule for
  sentinel_auth_keys_stale.

## 2026-10-27: gateway wiring (Postgres keys, PR 1d)

### Done
- loadConfig: SENTINEL_DATABASE_URL (unset/empty: env keys; set: Postgres
  only, SENTINEL_API_KEYS non-empty is an error; blank is an error),
  SENTINEL_KEYS_REFRESH (default 30s, >= 1s), SENTINEL_ADMIN_ADDR (default
  127.0.0.1:9465 with a DB, "off", an error without a DB except "off"),
  SENTINEL_ADMIN_TOKEN (required while admin is on, ValidateToken).
  parseMetricsAddr's port rule is now checkOwnPort, shared with admin,
  which must differ from the API's and the metrics' ports.
- serve: startKeys (Open 10 s, Migrate + SchemaVersion 30 s,
  NewRefreshing with the Recorder) before any listener; newMux takes the
  KeySource. Admin listener between metrics and the API, own http.Server
  (metrics' timeouts, DisableGeneralOptionsHandler). Shutdown: API, admin,
  metrics, cancel refresh, wait Done (5 s), close the pool.
- Compose: postgres in the mock profile (no host port, volume, TCP
  healthcheck, read_only, cap_drop ALL, no-new-privileges, user postgres,
  tmpfs for /var/run/postgresql and /tmp, dev-only password). Mock gateway
  on it with SENTINEL_API_KEYS="", admin on 127.0.0.1:9465, token from .env
  with ${...:?}. Makefile: a placeholder token for every target but up.
  CI's compose check: fails without the token, passes with one.
- Grafana: client keys, refresh age (90 s line), refresh errors.
- Tests: every config rule, no URL or token in any error; serve on a fake
  keyStore (issue via admin, 200 after a refresh, 401 after revoke and a
  refresh; OPTIONS * 401; admin not on the API port; bind and serve
  failures; startup failures and deadlines; store closed last); real
  store.Open on bad URLs leaks nothing; the same lifecycle on real
  Postgres with SENTINEL_TEST_POSTGRES_URL.
- Binary (linux, -s -w): 13.08 MB -> 17.28 MB (+32%); ADR 0005 has it.

### Decided
- An admin server failure keeps the API serving, as metrics does: keys
  loaded keep working and refreshing; only issuing and revoking stops.
- Migrations run automatically at every start; the role needs CREATE.
- SENTINEL_KEYS_REFRESH has a 1 s floor (fetch timeout is half of it).
  SENTINEL_KEYS_REFRESH and SENTINEL_ADMIN_TOKEN without a DB only WARN;
  SENTINEL_ADMIN_ADDR=off without a DB is accepted.
- The refresh's context is not the signal's, so keys refresh during the
  grace period; a signal during the first fetch still stops startup.
- If the refresh doesn't stop within 5 s, the pool is left open: Close
  would wait for the stuck fetch's connection.

### Verified
- 2026-10-27, make up smoke test: the postgres service starts as user
  postgres on a read-only root; a tenant and key issued through the admin
  API get 200 on the API after one refresh and 401 after revoke and a
  refresh; the key is in no log line; three audit lines;
  sentinel_auth_keys 1 -> 0, refresh errors 0; every Grafana panel shows
  data.

### After review (go-reviewer), tests first
- A signal during Open, Migrate or the first fetch: INFO "startup
  interrupted", serve returns nil (exit 0), not an ERROR exit. Tests for
  all three fail if the AfterFunc hook or the signal ctx is dropped.
- Shutdown order test: an admin call held in flight and a fetch blocked
  until cancelled; it asserts admin drained, refresh cancelled and
  returned, then Close. Fails if Close moves before the cancel, the wait
  on Done goes, or stopKeys runs before the admin shutdown.
- Open and Migrate deadlines tested in synctest (exactly 10 s and 30 s;
  a step without its WithTimeout deadlocks the bubble).
- keysStopTimeout and adminShutdownTimeout are vars: a fetch ignoring
  its ctx leaves the pool open with a WARN and shutdown completes; an
  admin request open past the admin grace period gets a WARN.
- The real-Postgres lifecycle test refreshes every 500 ms (fetch timeout
  250 ms) instead of 50 ms, against CI flakes.
- The base compose gateway blanks SENTINEL_ADMIN_TOKEN and
  SENTINEL_KEYS_REFRESH, so make up-real neither receives the token nor
  warns; the mock override sets the token and the refresh
  (${SENTINEL_KEYS_REFRESH:-30s}) again.
- The README admin example loads .env with set -a and sends the token
  through curl -H @- from a shell function.
- The DB URL is stored trimmed. CI's negative compose check requires the
  error to name SENTINEL_ADMIN_TOKEN. Grafana refresh errors panel:
  axisSoftMax 1.
- Database roles (ADR 0005): compose connects as the image's superuser;
  production should use a migration role that owns the schema and a
  runtime role without CREATE (SELECT, INSERT, UPDATE), which the
  to_regclass check in the runner allows.

### Next
- A separate migrate command, so the runtime role never needs CREATE.
- The admin API and the key refresh share one pgx pool (default size),
  so a token holder could starve refreshes: a pool of its own for the
  admin API, or an admin rate limit.
- Per-operator admin credentials (ADR 0005 limits).

## 2026-10-27: admin API package (Postgres keys, PR 1c)

### Done
- internal/admin: New(Config{Store, Token, Logger}) http.Handler; Store
  is an interface of the six store methods it calls, and a compile-time
  check holds *store.Store to it. Routes: POST/GET /admin/tenants, POST
  /admin/tenants/{id}/disable, POST/GET /admin/tenants/{id}/keys, DELETE
  /admin/keys/{id}. Lists are {"tenants": [...]} and {"keys": [...]},
  [] when empty. JSON 404/405 (with Allow) via method-less patterns and a
  "/" catch-all; an unclean path is a 400 before the mux, which would
  redirect it.
- Auth wraps the whole mux: bearer token, one Authorization header only,
  SHA-256 digests compared in constant time, the same 401 with
  WWW-Authenticate: Bearer and Connection: close for every path. WARN via
  proxy.LogThrottle (10 s). ValidateToken exported for 1d.
- Body rules, error mapping, no-store and nosniff on every response, one
  audit line per change. ADR 0005 has an Admin API section; the wiring
  references in it moved from 1c to 1d.
- Tests with a fake store and a JSON log buffer: every route and some
  unmatched ones against 16 bad Authorization variants (same body, no
  store call, no body read), the key only in the create response, error
  mapping on all six routes including an error text that must not leak,
  body limits at and over 4 KiB, a real httptest server. Mutations
  (accepting any bearer, a leaking 500, no body limit, logging the key)
  each fail tests.

### Decided
- PR 1c is the package only; 1d adds SENTINEL_ADMIN_ADDR (its own port,
  refused if it equals the API's, as the metrics address is), the token
  env var, the store, and compose.
- ValidateToken allows printable ASCII only (0x21-0x7e), stricter than
  "no whitespace or control": a non-ASCII token can be encoded differently
  by a client than by the config.
- Every response is no-store, not only the create-key one, which also
  sets it itself.
- New panics on a nil Store or Logger or an invalid token, since it
  returns no error; 1d validates the token first.
- POST disable and create-key take no input, but a body if sent must be
  application/json and decode into an empty object ({} or null).
- revoke_key's audit line has only key_id: RevokeKey returns no tenant or
  prefix. Reads are not audited.

### Next
- PR 1d: wire it (listener, server timeouts like the metrics one, token
  from env, store Open/Migrate with deadlines), compose, binary size.
- PR 1d: the admin http.Server sets DisableGeneralOptionsHandler: true,
  so OPTIONS * gets the handler's 401 instead of net/http's 200.
- Give LogThrottle a package of its own (keys and admin both use it).
- Per-operator admin credentials and a rate limit on admin, when needed
  (ADR 0005 known limits).
- ListTenants and ListKeys have no paging yet; add it (keyset on
  created_at, id) before tenant or key counts make a list response big.

### After review (go-reviewer), tests first
- Store calls get 5 s (storeTimeout). Changes use
  WithTimeout(WithoutCancel(r.Context())), so a client that disconnects
  can't cancel a change Postgres has committed and lose its audit line;
  reads use WithTimeout(r.Context()).
- Only a store error that is context.Canceled while r.Context() is done
  (a read the client canceled) writes nothing and logs WARN "admin client
  closed request" with status 499. A change's error is always ERROR and a
  500, client gone or not, since its context can't be canceled by the
  client. A synctest test checks a hung store: 500 and ERROR after exactly
  5 s, for a read and a change.
- The unauthorized WARN logs EscapedPath() (ASCII), cut at 256 bytes,
  with path_truncated and path_len when cut.
- Content-Type is checked only for a non-empty body, so an empty chunked
  body without one is no body. DELETE with a non-empty body is a 400.
  The clean-path check uses EscapedPath(), as Go 1.27's ServeMux does:
  %2F and %2e are part of an ID (a%2Fb is "a/b", %2e%2e is ".."), which
  the store doesn't find.
- Whitespace around the token moved to TestOverHTTP over raw TCP: net/http
  trims it, so it is accepted; whitespace inside is not. New tests: HEAD
  and OPTIONS on routes, an unauthenticated preflight (401, no CORS
  headers), escaped IDs, case-variant and duplicate JSON keys, null
  bodies, an empty chunked body, DELETE with a body, Expect: 100-continue
  (401 at once without the token, 100 then 201 with it).
- ADR 0005: the preflight gets 401, not 405; OPTIONS * is net/http's;
  JSON field matching and the escaped-path rule are documented.

## 2026-10-27: Postgres key store (Postgres keys, PR 1b)

### Done
- internal/store on pgx v5.11.0 (pgxpool): Open (parses, pings), Migrate,
  CreateTenant, ListTenants, DisableTenant, CreateKey (metadata plus the
  plaintext once), ListKeys, RevokeKey, ActiveDigests (a keys.Fetch: keys
  not revoked of tenants not disabled). Typed errors: ErrNotFound (also
  for a malformed UUID), ErrDuplicateName, ErrTenantDisabled,
  ErrInvalidName, ErrSchemaTooNew. Disable and revoke are idempotent and
  keep the first time.
- Migration 0001: tenants and api_keys as specified, plus an index on
  api_keys.tenant_id. Own runner: one tx per migration, advisory xact
  lock, newer-version refusal.
- Key format snl_ + 43 chars base64url (32 bytes); prefix is the first 12;
  digest is SHA-256 of the whole key, equal to proxy.DigestKeys.
- Errors: every pgx error becomes *store.Error with op, a fixed
  description, SQLSTATE and constraint; pgx's text is dropped, not
  wrapped. Unwrap keeps only context errors.
- make test-postgres (postgres:18.6-alpine3.24 on 127.0.0.1:5433), denied
  to Claude Code in .claude/settings.json; CI gets a postgres service on
  the same image. ADR 0005.
- Tests: unit (no DB) for sanitizing, key format and digest equality;
  integration behind SENTINEL_TEST_POSTGRES_URL, each test in its own
  schema (search_path in the URL): migrate twice, 5 pools migrating at
  once, newer schema refused, digest length check, lifecycle, disable,
  typed errors, wrong password leaks nothing, and end to end with
  keys.Refreshing and proxy.Auth (revoked key 401 after one refresh).

### Decided
- No sqlc and no goose, against the earlier plan: seven short queries, and
  a migration runner small enough to read. Revisit sqlc with the admin API.
- No columns for #1 or #24; ADR 0005 says where they go.
- The constant-time loop stays; a scratch benchmark put it at about 20 ns
  per digest (0.2 ms at 10,000 keys) against ~15 ns for a map at any size.
  Switch to a map in KeySet before tens of thousands of keys.

- After review (go-reviewer), tests first: migration transactions are
  READ COMMITTED whatever the default (a serializable default made a
  gateway that waited on the lock rerun 0001); schema_migrations is
  created only when to_regclass finds none, so an up-to-date DB needs no
  CREATE; SchemaVersion is 0 on a never-migrated DB; an applied migration
  whose name differs is ErrSchemaMismatch; CreateKey's INSERT ... SELECT
  takes FOR SHARE on the tenant, so it waits for a concurrent disable and
  returns ErrTenantDisabled instead of a dead key; the rollback after a
  failed migration has a 5 s deadline. Migrate calls migrate(ctx, fs.FS),
  so a test runs a 0002 that fails halfway. sanitize passes through only
  an *Error or a bare sentinel, never a wrapper. validName rejects
  anything not unicode.IsPrint and format characters (Cf). The concurrent
  test opens five pools, then releases them together.

### Next
- PR 1c: config (connection string from env, refresh interval), main.go
  choosing Static or the store, the admin API, compose, binary size.
- Decide whether the gateway migrates at startup or a separate command does.
- PR 1c: give Open and Migrate a context with a deadline. Ping to a
  blackholed host waits for the OS TCP timeout, and pg_advisory_xact_lock
  waits forever behind a stuck migrator (or set lock_timeout).
- The deny rules in .claude/settings.json match by prefix:
  Bash(make test-redis:*) and Bash(make test-postgres:*) don't catch
  "make test test-postgres" or "make -C . test-redis". CLAUDE.md is the
  real control; consider a hook that checks the targets.

## 2026-10-27: refreshable key source (Postgres keys, PR 1a)

### Done
- proxy.KeySource returns a *proxy.KeySet, an immutable set of SHA-256
  digests (NewKeySet copies). Auth asks it once per request, so a request
  is checked against one whole set. Digest compare, the constant-time loop
  over every digest and clientID (hex digest, so rate-limit buckets) are
  unchanged. A nil or empty set rejects every request. NewAuthSource takes
  a KeySource; NewAuth and RequireKey keep taking []string and wrap it.
  internal/proxy imports neither internal/keys nor any DB package.
- internal/keys: Parse (SENTINEL_API_KEYS's rules, moved from main.go) and
  Static; Refreshing, which fetches digests on an interval and swaps them
  in with an atomic.Pointer. The first fetch must succeed or the
  constructor returns its error and starts nothing. A failed refresh keeps
  the last good set, is recorded and logged at WARN through
  proxy.LogThrottle (10 s). A fetch cut off by
  shutdown is not a failure. Done() closes when the loop stops on ctx
  cancel. Becoming empty logs one WARN.
- proxy.Recorder gained RecordKeySetSize and RecordKeyRefresh. Telemetry:
  sentinel_auth_keys, sentinel_auth_key_refresh_errors_total and
  sentinel_auth_key_refresh_age_seconds, no labels. Static reports only the
  size, so it has no age or error series; a refreshing source's error
  series exists at 0 from its first success.
- main.go uses keys.Static; Refreshing has no caller until PR 1b.
- Tests under -race: a removed key rejected after the next refresh, swaps
  racing concurrent requests, a failed refresh keeping the set, empty sets,
  cancel (also mid-fetch) stopping the loop, a failed first fetch starting
  nothing. The refresh tests run in testing/synctest, which also fails on a
  leaked goroutine. Existing auth tests unchanged.

- After review (go-reviewer): the ticker is now a timer reset after each
  refresh returns, so a hung fetch is followed by a full Interval of rest,
  not another fetch at once. Config.FetchTimeout (default
  min(Interval/2, 10s), must be positive and below Interval) bounds every
  fetch, the first too: a hung store fails startup with "the key store did
  not answer in time". refresh checks ctx before fetching; a ctx already
  done fails NewRefreshing before any fetch. Keys() on a nil Static or
  Refreshing returns nil and rejects. Config.Now drives the warning
  throttle, so synctest tests run it after its base. proxy.DigestKeys is
  the one place keys become digests. Tests added for each, written to fail
  first for the hung fetches and the fetch after cancel.

### Decided
- logThrottle stays in internal/proxy; LogThrottle exports it with Allow
  for internal/keys, which already imports proxy. Moving it to a package
  of its own would have meant changing the existing auth tests. A known
  smell: revisit if a third user appears.
- An empty first fetch is accepted: the gateway starts and fails closed,
  rejecting every request, with one WARN. Refusing to start would turn
  "every key revoked" into "gateway down".
- One key source per process, so the key gauges have no source label by
  design; a second source would overwrite the first's values.
- Review: go-reviewer with a focused brief on every PR; the multi-agent
  code-review skill on each dev->main release diff.

### Next
- PR 1b: Postgres fetch (pgx, sqlc, goose) behind keys.Refreshing, ADR 0005.
  - Sanitize pgx errors in the Fetch (connect errors carry user, database
    and host:port) and test that no connection string reaches a log line.
  - With a large key set, look the digest up in a map instead of the
    linear constant-time loop; record the trade-off in the ADR.
  - Alert on sentinel_auth_key_refresh_age_seconds, not only on refresh
    errors: a Fetch that ignores ctx records no failure.
- Dashboard panels for the key metrics.

## 2026-10-25: fail-open windows in a bench

### Done
- bench/RESULTS.md has a caveat: a make bench-gateway run, watched with
  the new metrics, had 4,058 of about 47k requests (8.6%) fail_open.
  gateway-bench logged "redis not reachable at startup" and the breaker
  opened four times during the 100 and 500 req/s stages. The log's
  fail-open WARN lines plus their suppressed counts give 4,054; the last
  batch is never logged. Tracked in #42.

### Learned
- HTTP statuses don't show a fail-open window: a fail-open request goes
  ahead and is answered like any other, and while the breaker is open it
  skips the Redis call. Earlier overhead figures may be optimistic, mostly
  at 100 req/s.
- Grafana's runtime plugin preinstall broke the dashboard on a read-only
  root: its update removed the bundled Prometheus plugin and couldn't
  write the new one. Preinstall is now off; the image's copy loads.

### Next
- After every bench, check
  sentinel_ratelimit_decisions_total{outcome="fail_open"}, not only HTTP
  statuses. #42 is to make make bench-gateway check it and reject the run.

## 2026-10-25: OpenTelemetry metrics

### Done
- proxy.Recorder: the Handler reports one RequestInfo per request from the
  deferred block that writes the log line (upstream, protocol, status,
  failure phase, rate-limit outcome, which stages ran and their
  durations, total). Options.Recorder nil costs a nil check. The log line
  and the wire bytes are unchanged; internal/proxy imports no OTel.
- proxy.Auth, built once by NewAuth and shared by every route (RequireKey
  stays as the one-route form). It reports each rejection to the Recorder
  and logs "unauthorized" at most once per 10 s with a suppressed count,
  through the same throttle as the fail-open warning. From #6.
- Admission.InUse and Limit, Limiter.BreakerOpen, for observable gauges.
- internal/telemetry: OTel SDK and the Prometheus exporter on a registry
  of its own, translation pinned. Requests, request duration, stage
  duration, gateway overhead, rate-limit decisions, auth rejections,
  admission in use and limit, breaker open. Attribute sets precomputed
  per upstream, status class and phase; recording a request is 0 allocs,
  about 1.5 µs.
- SENTINEL_METRICS_ADDR, default 127.0.0.1:9464, GET /metrics only, on its
  own listener; "off" disables. Binds before the API and logs "metrics
  listening"; shuts down after it. run() split into loadConfig and
  serve(ctx, logger, cfg), so tests start both listeners on port 0.
- Compose: prom/prometheus:v3.15.1 scraping gateway:9464 and
  gateway-bench:9464; grafana/grafana:13.2.4 on 127.0.0.1:3000 with
  anonymous Viewer, login form and basic auth off, a provisioned data
  source and dashboard (observability/). Both read_only, cap_drop ALL,
  no-new-privileges.
- ADR 0004. Tests: each proxy path recorded once with the right phase,
  status and outcome; the auth WARN throttled across routes; instruments
  and attributes through a ManualReader; overhead math; 1,000 distinct
  keys and models adding no series; the dashboard's names and label values
  present in a real scrape; the metrics listener, 404 for /metrics on the
  API port, off, and bad or taken addresses; benchguard with the new lines.
- Smoke test (make up, k6 at 1000 req/s): k6, Prometheus and the log all
  counted 47959 requests, 47811 2xx and 148 503 at admission. Every
  Grafana panel failed: "datasource prometheus was not found".
- Review fixes: a provider panic is recorded and logged as a failure (500
  unless a status was sent, with the stack), then re-raised. A metrics
  Serve error is logged and the API keeps serving. Default port 9464
  (9091 is the Pushgateway's). Metrics and API ports compare as numbers on
  any host. logThrottle measures from a monotonic base. Grafana: no
  runtime plugin installs or updates, no initial admin, no Gravatar, no
  external snapshots; Prometheus keeps at most 1 GB.

### Decided
- OTel over client_golang alone or hand-written exposition: one
  vendor-neutral API, one SDK for traces next, and the exporter can become
  OTLP without touching a recording call. The Prometheus exporter is built
  on client_golang, so client_golang is a direct dependency either way.
- Labels only from closed sets, and anything else becomes "other".
  RequestInfo doesn't even carry the model or the key.
- 401s are counted by sentinel.auth.rejections, not sentinel.requests; the
  Handler never sees them.
- Overhead is recorded only for forwarded requests; for a 429 it would be
  the whole request. The Redis call counts as overhead.
- A client gone during the rate-limit check is outcome not_checked: no
  decision was made and no fail mode applied.

### Learned
- Grafana 13.2 ships the Prometheus data source as a plugin, not core: a
  copy bundled in the image (data/plugins-bundled) plus a background
  "preinstall" that updates it from grafana.com at startup. With a
  read-only root the update removed the plugin and couldn't write the new
  one. plugins.preinstall_disabled turns the whole preinstall off; the
  bundled copy still loads, since paths.bundled_plugins is always on the
  plugin path (pkg/setting/setting.go, v13.2.4).
- Re-panicking from a deferred recover loses the panic site in net/http's
  log, so the handler logs debug.Stack() before re-panicking.
- Windows' monotonic clock is coarse enough that a fast stage measures 0,
  so RequestInfo says which stages ran (Ran) instead of relying on
  durations above zero.
- Passing a precomputed metric.WithAttributeSet option as a lone variadic
  argument still allocates the slice, 16 B per call (measured); passing a
  precomputed []metric.AddOption with ... allocates nothing.

### Next
- Traces on the same SDK, with exemplars linking duration buckets to them.
- Go runtime metrics (contrib runtime instrumentation).
- Measure the recording cost under load in make bench-gateway.
- A client that disconnects while uploading its body is logged and
  recorded as 400 read_body, not 499. Tell it apart from a malformed body.

## 2026-10-25: provider hardening

### Done
- Provider contract (#5): a nil error from Forward means a non-nil
  Response with a non-nil Body and a status from 200 to 599. The Handler
  checks it and answers a breach with 502 "upstream unavailable", logged
  with phase upstream, closing a Body it got. No panic.
- RequestError.Status: zero means 400, other 4xx values are honored,
  anything else becomes 400.
- The Handler strips hop-by-hop response headers (and any header named in
  Connection) itself, whatever the provider's allowlist lets through.
- Upstream error bodies (#33): each provider reads a non-2xx body up to
  64 KiB and re-encodes its own error shape with the message's account
  identifiers redacted (org-..., proj_..., UUIDs); request IDs stay.
  Anything else is replaced with the gateway's "upstream error" at the
  same status. 2xx bodies are never read. Shared ReadErrorBody and
  RedactAccountIDs in internal/provider/upstream.
- upstream.NewClient no longer panics when http.DefaultTransport has been
  replaced (from #6); it falls back to a fresh transport with Go's
  documented defaults.
- Review fixes: an upstream 1xx (Go hands back a 101) is a Forward error,
  502, with no body read. ReadErrorBody takes the context, closes the body
  when it ends (context.AfterFunc), and has its own 10 s deadline, a 502
  logged as "upstream error body not received in time" instead of the
  header timeout's 504. The Handler accepts only 2xx, 4xx and 5xx; a 300,
  a 304 or a 3xx without Location is a 502. copyEndToEnd writes canonical
  keys, so a provider's x-request-id can't sit beside the gateway's.
  RequestError's 401, 407, 429 and 499 become 400. Redaction is
  case-insensitive, catches IDs after "_" or a digit, and covers sk-...,
  wrkspc_..., apikey_... and 32-hex UUIDs. OpenAI's numeric code keeps
  its digits (json.Number).

### Decided
- Sanitizing stays in the providers, not the core: only a provider knows
  its error shape, and replacing every upstream error with a generic one
  would hide messages developers need.
- Re-encode rather than edit in place: unknown fields are dropped, so an
  identifier in a field nobody looked at can't slip through. OpenAI's
  param and code are kept, and a numeric code from a compatible server
  too.
- 401/403 bodies are sanitized as well, though the core replaces them;
  the provider rule is simply "every non-2xx body".
- A failed read of an error body (connection reset, its own timeout) is a
  Forward error, so the client gets the core's 502 (499 if it left), not a
  made-up body at the upstream's status.
- A RequestError can't become a 5xx: a provider rejecting the client's
  request must not look like a gateway failure to SDK retry logic or to
  the log level.

### Learned
- encoding/json escapes <, > and & by default, so a re-encoded message
  like "max_tokens: 100000 > 64000" changed bytes; the re-encoder turns
  that off.
- A 101 response turns the body into the raw connection, and cancelling
  the request no longer stops a read on it: the first error-body reader
  hung forever on one. Every stage that reads from the network needs its
  own deadline, enforced by closing what it reads from, not only a
  context.

### Next
- Sanitize error events inside an SSE stream after a 200 (documented as a
  known limitation).
- Show the operator the sanitized upstream reason when the gateway's own
  key is rejected (401/403). The core drops that body today; it needs a
  Response field for the provider to pass the reason in.
- The other nits in #6: timer Stop races, flushing only for
  text/event-stream, the query string, sampling the auth WARN.

## 2026-10-24: named upstreams

### Done
- Protocol and upstream are split in cmd/gateway: a protocol (anthropic,
  openai) is a path and a Provider constructor; an upstream is a name, a
  protocol, a base URL and a key. The built-ins keep ANTHROPIC_* and
  OPENAI_*. Any other name in SENTINEL_UPSTREAMS reads
  SENTINEL_UPSTREAM_<NAME>_PROTOCOL, _BASE_URL and _API_KEY, all required.
- Every upstream gets POST /<name>/v1/messages or
  /<name>/v1/chat/completions; the built-ins keep the unprefixed routes
  too. Same client keys, rate limiter and Admission for all.
- The one change in internal/proxy: Options.Upstream, logged as "upstream"
  next to "provider". Defaults to the provider's Name().
- loadConfig takes environ as well as getenv, to find
  SENTINEL_UPSTREAM_<NAME>_* variables for unlisted names (one WARN per
  name, no values) and SENTINEL_UPSTREAM_ANTHROPIC_* / _OPENAI_* (an
  error). The startup line also logs the protocol now.
- make bench-gateway checks what gateway-bench resolved: it waits for
  "listening", then cmd/benchguard reads the "upstream enabled" lines and
  refuses unless each has exactly one base_url equal to the mock's
  scheme://host:port. gateway-bench and make up gain
  a custom upstream, mockcompat, and k6 takes BENCH_UPSTREAM (a bare name).
- ADR 0003. Tests: names, missing and bad variables, unlisted and
  built-in variables, no secrets in errors or logs, two upstreams of one
  protocol each reaching only its own server and key, unprefixed routes
  only for built-ins, 404 + close on an unknown prefix, one Admission
  across prefixes, the log fields, and the guard's matching.
- Review fixes: upstream clients refuse redirects (502, logged as
  "upstream redirect refused"); a request path that isn't clean gets a
  plain-text 400 with Connection: close before the mux; "upstream
  enabled" and the CA warning log only scheme://host[:port]; the CA
  warning covers any upstream on a public default host;
  SENTINEL_UPSTREAM_* names match ignoring case; unknown suffixes on a
  listed upstream warn; "listening" is logged only after the bind;
  benchguard fails closed on unreadable or duplicate-key lines.
- Forward errors no longer carry the request URL: upstream.StripURL drops
  it from http.Client's *url.Error, keeping the cause wrapped. The log
  read `Post "https://host/k/<token>/v1/...": ...`, so a token in a base
  URL's path, or a refused redirect's target, reached the request log.

### Decided
- Route by path prefix, not by model name. The path is fixed by
  configuration and fails closed with a 404; a model table would be
  client-driven and always out of date.
- No defaults for custom upstreams. A guessed default could send one
  provider's key to another's API.
- Names are [a-z][a-z0-9]{0,31}, no hyphens or underscores, so each
  variable maps back to one name. "v1" and "ca" are reserved.
- Environment variables, not a config file, until upstreams live in
  Postgres per tenant.
- A bad PROTOCOL value is not echoed in its error, since it could be a
  mispasted key. Bad names in SENTINEL_UPSTREAMS still are, as before.
- The default-host CA warning applies to any upstream on api.anthropic.com
  or api.openai.com, built-in or custom.

### Learned
- An env-var bench guard has to know every upstream in advance; the
  gateway's own "upstream enabled" lines are what it will actually call.
- On Windows, make runs a recipe line with no shell syntax directly, not
  through sh, so a fake docker script on PATH is not enough to test a
  recipe; the compose command had to be overridden as well.
- Git Bash rewrote BENCH_PREFIX=/mockcompat into a Windows path, and
  every request got 404 in under 1 ms. Inputs should not start with "/".
- Counting log lines by "upstream" also matches the startup line; filter
  on "msg":"request" first.
- The review found two problems that predate this branch and that named
  upstreams make easier to reach: the upstream client followed redirects
  and leaked X-Api-Key, and unclean paths hung before auth.

### Next
- Per-upstream CA file, then per-upstream rate limits.
- Upstream error-body sanitization, one provider per PR.
- A 401 message that doesn't say x-api-key on the OpenAI route.
- Per-key concurrency cap.

## 2026-10-24: OpenAI-compatible protocol

### Done
- New package internal/provider/openai: POST {base}/chat/completions with
  the gateway's bearer key. Default base https://api.openai.com/v1, with
  /v1 in it as in the OpenAI SDKs. Uses internal/provider/upstream for the
  base URL rule and the client.
- No file under internal/proxy changed. The seams from the refactor were
  enough: Provider, ErrorFormatter, shared Admission.
- SENTINEL_UPSTREAMS picks the upstreams (default anthropic); each one's
  key is required only when it is listed. A route is registered only for
  an enabled upstream. cmd/gateway builds the mux in newMux, which tests
  call, with one Admission and one rate limiter for every route.
- mockupstream serves /v1/chat/completions too, streaming chunks and
  data: [DONE]. k6 takes BENCH_API=openai. make up and gateway-bench enable both
  upstreams on the mock with dummy keys; make bench-gateway checks both
  base URLs before it runs.
- Tests: headers sent upstream, response allowlist, OpenAI error bodies,
  streaming and request IDs through RequireKey and the handler, from an
  external test package; SENTINEL_UPSTREAMS parsing; routes per upstream;
  a 503 on one route while the other holds the only slot.

### Decided
- Forward no client header on the OpenAI route. OpenAI-Organization and
  OpenAI-Project choose who is billed and must come from the gateway, if
  anywhere. OpenAI-Beta and X-Stainless-* have no use upstream worth the
  risk of an allowlist we would have to maintain.
- The upstream's X-Request-Id is dropped from the response and logged as
  upstream_request_id. The client keeps the gateway's own X-Request-Id.
- OpenAI error type and code strings are informative only: OpenAI SDKs
  pick the exception from the HTTP status. Not every value is checked
  against OpenAI's live responses.
- An upstream key set for an unlisted upstream is ignored with a WARN, not
  an error, so one .env can serve several setups.
- The extra-CA warning fires per enabled upstream on its public default
  host, compared by hostname so a trailing slash, a trailing dot or a path
  doesn't hide it.
- Upstream error-body sanitization goes in each provider, in its own PR,
  not in the core. Until then error bodies pass through as-is.
- Review fixes: base URL errors name at most the scheme and hostname, and
  never wrap url.Parse's error, which repeats the URL. Upstream keys with
  edge whitespace or control characters stop startup. Only base URL
  errors are blamed on the base URL variable; other provider errors read
  "upstream <name>: ...".

### Learned
- "OpenAI-compatible" covers most local model servers (Ollama, vLLM), so
  one provider reaches far more than OpenAI. Only loopback gets plain http,
  which suits a local Ollama but not one in another container.
- Azure OpenAI, Bedrock and Vertex differ in auth, not just in base URL, so
  they need providers of their own.
- A load test that returns only 200s does not prove which route it hit.
  Count the gateway's log lines by provider, as was done for
  BENCH_API=openai.
- A 2 s bench right after the container is recreated shed about 8% at
  1000 req/s with 503 at admit, during a machine stall (one Redis call
  took 7.4 ms against a 249 us median). Use 5 s or more for smoke runs.

### Next
- Named upstreams, so OpenAI and a local server can be served side by side.
- Upstream error-body sanitization, one provider per PR.
- A 401 message that doesn't say x-api-key on the OpenAI route. It comes
  from RequireKey, so it means a change in internal/proxy.
- Per-key concurrency cap.

## 2026-10-24: provider seams

### Done
- Refactor ahead of a second protocol and named upstreams. Nothing changes
  on the wire for the Anthropic route.
- The concurrency cap is now a proxy.Admission, created once in
  cmd/gateway and shared by every handler. A handler without one still
  makes its own from MaxConcurrent.
- Error responses are rendered by the protocol: the core has error kinds
  and an ErrorFormatter interface that Provider embeds, and RequireKey
  takes one. The Anthropic provider maps the kinds to its error types.
- A characterization test pins the exact bytes, headers and status of
  every error the gateway emits, from 401 to 504. Written and run green
  before the refactor, unchanged after it.
- New package internal/provider/upstream: base URL rule, loopback check,
  pooled TLS client with extra CAs, header control-character check. It
  does not import internal/proxy. Anthropic's errors keep their text.
- .claude/settings.local.json and .claude/worktrees/ in .gitignore.

### Decided
- One cap for the whole gateway, not one per route. A cap per route would
  multiply the memory bound in ADR 0001 by the number of routes.
- The core owns error meaning, the protocol owns error shape. An OpenAI
  SDK reads error.message from Anthropic's shape, but the type values are
  wrong, and other clients may not parse it at all. The 401 from
  RequireKey is the first thing a misconfigured client sees.
- Programmer errors panic, like http.Handle with a nil handler: a nil
  formatter or a zero-value Admission at construction, an unmatched
  Release when it is called.
- Shared upstream code takes MaxIdleConns from the caller rather than
  importing proxy for the default, so any protocol can use it.

### Learned
- net/http writes the handler's headers sorted, then Date and
  Content-Length, so a raw-bytes golden test is stable once Date and
  X-Request-Id are masked.

### Next
- OpenAI-compatible provider on these seams.
- Named upstreams, each with its own base URL and key.
- Per-key concurrency cap, so one key can't take every upstream slot.

## 2026-10-23

### Done
- Repo made public. README with architecture diagram.
- CI on every PR: gofmt, go vet, go test -race.
- Load test with k6 against a mock upstream (100 ms delay). Median gateway
  overhead about 1 ms or less up to 1000 req/s. Results in bench/RESULTS.md.
- Concurrency cap made configurable (SENTINEL_MAX_CONCURRENT). ADR 0001.
- Private CA support for the upstream (SENTINEL_UPSTREAM_CA_FILE) and
  cmd/devcerts, so the mock runs over HTTPS. The https-only rule is unchanged.
- Docker Compose dev environment and Makefile. make bench-gateway runs only
  against a gateway fixed to the mock and refuses to start otherwise.
- Per-key rate limiting in Redis (PR #25, ADR 0002). A token bucket per
  client key in one Lua script, atomic and on Redis's clock. 429 with
  Retry-After, checked before the body is read. The Redis key is a SHA-256
  digest of the client key, never the key itself. Every Redis call is
  bounded at 50 ms. Fail open by default, fail closed as an option. First
  external dependency: go-redis (miniredis for tests only).
- Circuit breaker in front of Redis: after 5 failures in a row it skips
  Redis for 5 s, then one probe decides whether it closes. A review found it
  could flap when a call already in flight succeeded after it opened; now
  only the probe changes its state. Running out of pooled connections no
  longer counts as a Redis failure.
- Rate-limit tests also run on a real Redis (CI service, make test-redis),
  since miniredis runs Lua with a different interpreter.

### Decided
- Tail spikes in the load test point at the shared test machine; treat p99
  as an upper bound until k6 runs on a separate machine.
- Mock over HTTPS with a private CA, instead of a flag that allows plain http.
- Merge dev into main with a merge commit, never squash. An earlier squash
  split the histories; fixed with a merge -s ours on dev.
- Rate limiting fails open by default: it protects fair use and cost, not
  confidentiality, and fail closed would make Redis as critical as the
  gateway.

### Learned
- Changes to .env need make up again; compose reads it only at start.
- The bench target could have hit the real API with the real key. Guard
  added, and auto-reload stays off on the Anthropic account.
- The Redis call costs a median of 249 µs and a p99 of 1.2 ms in a short
  bench run. A first number, not a full bench result.
- An unsalted digest in Redis can be brute-forced for short client keys.
  Issue #24: derive the bucket key with HMAC and a gateway secret.

### Next
- Per-key concurrency cap, so one key can't take every upstream slot even
  within its rate limit.
- OpenAI provider behind the same interface.

## 2026-10-08

### Done
- Minimal Anthropic proxy in Go, stdlib only, behind a Provider interface.
- Gateway auth, streaming with per-chunk flush, first-byte and idle timeouts,
  header allowlist, resource limits, structured logs with per-stage timings.
- Two rounds of review with a reviewer agent; deferred findings filed as issues.

### Decided
- Gateway binds to 127.0.0.1 by default.
- Running streams are drained on shutdown up to the grace period.
