# Sentinel

Security gateway for LLM apps and MCP agents. A proxy in front of LLM
providers that detects prompt injection, redacts secrets/PII, enforces
tool policies, and writes an audit log.

## Stack
- Gateway: Go, net/http, log/slog
- Upstreams: two protocols, Anthropic and OpenAI-compatible
  (internal/provider/openai), behind a Provider interface; any number of
  named upstreams per protocol (docs/adr/0003-named-upstreams.md)
- Providers own their non-2xx bodies: each re-encodes its protocol's error
  shape with account IDs redacted, or replaces the body, using
  internal/provider/upstream (ReadErrorBody, RedactAccountIDs). 2xx bodies
  are never read. The core strips hop-by-hop headers as a backstop.
- Metrics: OpenTelemetry in internal/telemetry, exposed with the OTel
  Prometheus exporter (built on prometheus/client_golang). The core calls a
  proxy.Recorder (RequestInfo per request, RecordAuthRejection per 401,
  RecordKeySetSize, RecordKeyRefresh and RecordKeysStale from the key
  source) and never
  imports OTel (docs/adr/0004-opentelemetry-metrics.md). Labels only from
  closed sets: never the client key, its digest, the model or the path.
  The key metrics have no labels: one key source per process.
  Traces come later on the same SDK.
- Client keys: proxy.Auth reads them from a proxy.KeySource, once per
  request, as an immutable proxy.KeySet of SHA-256 digests (constant-time
  compare over every digest; clientID is the hex digest). A nil or empty
  set rejects everything. internal/keys implements it: Static (from
  SENTINEL_API_KEYS, without a database) and Refreshing (a Fetch of digests on an
  interval, atomic swap, last good set kept on failure, first fetch must
  succeed within FetchTimeout; MaxStale: past it since the last good
  fetch, Keys() returns proxy.UnavailableKeySet() and Auth answers every
  request 503, ERROR once in, INFO once out). A Fetch must honor ctx and
  return errors safe to log. internal/proxy never imports internal/keys
  or a DB package.
- Key store: internal/store, Postgres via pgx/pgxpool, no sqlc
  (docs/adr/0005-postgres-client-keys.md). Tenants and api_keys (UUID ids,
  only SHA-256 digests, nothing deleted: disabled_at, revoked_at).
  Store.ActiveDigests is a keys.Fetch. Keys are "snl_" + base64url of 32
  random bytes, returned once by CreateKey, never stored or logged.
  Migrations: embedded internal/store/migrations/NNNN_name.sql, own runner
  (schema_migrations, one READ COMMITTED tx per migration under
  pg_advisory_xact_lock, ErrSchemaTooNew for a newer DB,
  ErrSchemaMismatch for a different applied name; an up-to-date DB needs
  no CREATE right), forward only. Every error is a
  sanitized *store.Error (op, SQLSTATE, constraint; never pgx's text,
  which names user, database or host) or a typed error.
- cmd/gateway with SENTINEL_DATABASE_URL: startKeys does store.Open (10 s)
  and Migrate (30 s, automatic on every start), then keys.NewRefreshing
  on ActiveDigests with the Recorder, all before any listener binds; any
  failure stops startup, except that a signal during them logs INFO
  "startup interrupted" and returns nil (only when the error is Canceled
  or DeadlineExceeded and the signal ctx is done). It sees the store as a keyStore
  interface (openStore is a var, so tests use a fake). The refresh ctx is not the
  signal's; shutdown: admin stops accepting at once, alongside the API
  drain (30 s), then admin.API.Drain waits for its handlers (5 s grace,
  WARN if cut, then up to 6 s; still running: WARN, pool left open);
  metrics, then cancel the refresh, wait for Done (5 s; if it doesn't
  stop, WARN and leave the pool open), close the pool. Compose connects as the image's superuser;
  production: a migration role and a runtime role without CREATE (ADR
  0005, Database roles). Admin and refresh share one pool of
  store.MaxConns = 8 (overrides pool_max_conns); admin.MaxInFlight =
  MaxConns/2 = 4, past it a 503 with Retry-After at once.
- Admin API: internal/admin, New(Config{Store, Token, Logger})
  *admin.API (an http.Handler with Drain) over an admin.Store interface (*store.Store satisfies it),
  on its own listener (SENTINEL_ADMIN_ADDR, like the metrics one; its
  http.Server has DisableGeneralOptionsHandler), never the API port.
  Bearer token checked first (constant-time over SHA-256 digests; same
  401 for every path; before the body is read), ValidateToken (>= 32
  printable ASCII, no space). JSON in and out, one error shape
  {"error":{"type","message"}}, 4 KiB bodies, application/json only,
  unknown fields and trailing values rejected; 500s say only "internal
  error". Store calls get 5 s; changes ignore the client's cancellation
  (WithoutCancel); a read the client canceled is a WARN 499, any other
  store error ERROR and 500. The plaintext key is only in the create-key 201; every response
  is no-store. One INFO "admin" line per change (action, ids, prefix,
  actor "admin-token"), never the key or token.
- Later: Kafka (franz-go), ClickHouse, Python detector service over gRPC

## Principles
- The hot path must be fast and measurable. Every stage records its latency.
- Keep it simple: stdlib first, add a dependency only with a reason. So
  far: go-redis (docs/adr/0002-rate-limiting.md) and OpenTelemetry with its
  Prometheus exporter (docs/adr/0004-opentelemetry-metrics.md), pgx
  (docs/adr/0005-postgres-client-keys.md); miniredis is test-only.
- Every change comes with tests.
- Never commit secrets. API keys come only from environment variables.
- Claude Code never runs docker, docker compose, or make targets that start
  containers, and never reads .env. Ask the user to run them.

## Commands
- go run ./cmd/gateway (needs the env vars below), or make run
- go test ./... (make test adds -race; make vet, make fmt). make test-redis
  also runs the rate-limit tests on a throwaway redis:7-alpine container;
  SENTINEL_TEST_REDIS_URL enables them by hand, and CI sets it. make
  test-postgres does the same for the store tests on a throwaway
  postgres:18.6-alpine3.24 on 127.0.0.1:5433 (SENTINEL_TEST_POSTGRES_URL;
  CI sets it too). Each store test uses a schema of its own.
- make (or make help) lists the targets.
- make up: compose stack with gateway -> mockupstream over HTTPS, plus
  postgres for the client keys (admin API on 127.0.0.1:9465; needs
  SENTINEL_ADMIN_TOKEN in .env), redis for rate limiting, and Prometheus
  and Grafana (127.0.0.1:3000,
  anonymous Viewer, provisioned from observability/) for metrics. The gateway serves both protocols there, with
  anthropic, openai and the named upstream mockcompat (openai) all on the
  mock. Runs make certs first, which rewrites ./certs when a
  file is missing or server.pem expires within 7 days, and recreates the
  containers. Gateway on 127.0.0.1:8081 only. make up-real: gateway
  against the real APIs in SENTINEL_UPSTREAMS from .env (default
  anthropic). make down, make logs. Every target but make up passes a
  placeholder SENTINEL_ADMIN_TOKEN when the shell has none, so they work
  without one in .env (compose checks ${SENTINEL_ADMIN_TOKEN:?} on any
  command reading docker-compose.mock.yml; CI's config check sets one).
- make bench: k6 in a container against the mock directly (after make up).
  make bench-gateway: k6 through gateway-bench, a separate gateway fixed to
  the mock for all its upstreams (anthropic, openai, mockcompat) with dummy
  upstream keys. It checks resolved config, not env vars: it waits (about
  15s) for gateway-bench's "listening" line (the metrics listener binds
  first and logs "metrics listening", which must never match), then
  cmd/benchguard refuses
  to run unless there is at least one "upstream enabled" line and each has
  exactly one base_url (logged as scheme://host:port), equal to
  https://mockupstream:9090; a line it can't read refuses too. After k6,
  cmd/benchverdict rejects the run (exit 1, one "bench rejected:" line)
  unless gateway-bench's own counters show 0 fail_open, allowed equal to
  k6's http_reqs count, no other outcome, and its log has "listening" and
  no "redis not reachable at startup"; missing or unreadable metrics
  reject too. A clean run prints "bench clean: N requests, N allowed, 0
  fail_open" and exits with k6's status. k6 runs as k6-gateway (extends
  k6, volume k6-out on /home/k6): K6_SUMMARY_EXPORT writes a per-run
  summary file there, then bench/k6/gateway-verdict.js prints it with
  gateway-bench:9464/metrics between markers, from the compose network (no
  published port). BENCH_API
  ("anthropic" or "openai"), BENCH_UPSTREAM (a bare name such as
  mockcompat, never a path: Git Bash rewrites a leading "/"), DURATION_S
  and STREAM pass through.
- Compose without make: docker compose up (real), or add
  -f docker-compose.yml -f docker-compose.mock.yml --profile mock (mock).

## Configuration (environment variables)
- SENTINEL_UPSTREAMS (optional): comma-separated upstreams to serve: the
  built-ins anthropic (POST /v1/messages) and openai
  (POST /v1/chat/completions), and named upstreams; default anthropic.
  Names match ^[a-z][a-z0-9]{0,31}$; "v1" and "ca" are reserved. A bad name, a
  duplicate, or a list with no names stops startup. Every enabled upstream
  gets POST /<name>/v1/messages or /<name>/v1/chat/completions by protocol;
  only the built-ins also get the unprefixed route. A route exists only for
  an enabled upstream; all routes share the client keys, the rate limiter
  and one proxy.Admission. One INFO line per enabled upstream logs its
  name, protocol and base URL as scheme://host[:port] only (a path may
  hold a token), never the key. The request log has
  "upstream" (the name, via proxy.Options.Upstream) and "provider" (the
  protocol's Name()). A request path that isn't already clean (//, a dot
  segment, a trailing slash) gets a plain-text 400 with Connection: close
  before the mux, never a redirect. Upstream clients never follow a
  redirect (upstream.ErrRedirect): the client gets a 502. Forward errors
  go through upstream.StripURL, so the logged error never holds a URL.
- SENTINEL_UPSTREAM_<NAME>_PROTOCOL, _BASE_URL, _API_KEY (all required for
  a listed named upstream, NAME upper-cased): "anthropic" or "openai", the
  base URL (no default; same https rule and error rules as the built-ins),
  and the gateway's key for it. SENTINEL_UPSTREAM_* names are matched
  ignoring case, as Windows does. Any SENTINEL_UPSTREAM_ANTHROPIC_* or
  SENTINEL_UPSTREAM_OPENAI_* variable stops startup. Variables of an
  unlisted NAME are not validated or used; one WARN per name, no values.
  A listed NAME's variables with another suffix (_APIKEY) get one WARN per
  upstream, naming them without values.
  loadConfig takes getenv and environ, so tests stay hermetic.
- ANTHROPIC_API_KEY (required when anthropic is listed): the gateway's own
  upstream key. Clients never see it.
- OPENAI_API_KEY (required when openai is listed): the gateway's own key for
  the OpenAI-compatible upstream. A key set for an unlisted upstream is not
  used, and a WARN at startup says so. An enabled upstream's key with
  leading or trailing whitespace or a control character stops startup; the
  error names the variable, never the key.
- SENTINEL_API_KEYS (required without SENTINEL_DATABASE_URL):
  comma-separated client keys. The gateway refuses to start without at
  least one; with a database it must be empty (never both). Clients send a key as `x-api-key`
  or `Authorization: Bearer <key>`; anything else gets a 401.
- SENTINEL_DATABASE_URL (optional): Postgres for the client keys. Unset or
  empty: keys from SENTINEL_API_KEYS. Set: keys from Postgres only. Blank
  (spaces) stops startup. No error or log ever holds the URL. pgx also
  honors PG* variables and ~/.pgpass.
- SENTINEL_KEYS_REFRESH (optional): key fetch interval with a database,
  Go duration, default 30s, at least 1s. Revocation latency: within it
  plus up to two fetch timeouts (min(refresh/2, 10s) each), so at most 2x
  the refresh, 50 s with the defaults. Set without a database: one WARN,
  ignored.
- SENTINEL_KEYS_MAX_STALE (optional): how long the last good key set is
  served while refreshes fail, default 10m, 0 never expires. Past it every
  request gets 503 (sentinel_auth_keys_stale 1). Negative or under three
  times SENTINEL_KEYS_REFRESH (the default too) stops startup;
  keys.NewRefreshing needs MaxStale >= 2 x (Interval + FetchTimeout). Set without a
  database: one WARN, ignored.
- SENTINEL_ADMIN_ADDR (optional): admin API listener, default
  127.0.0.1:9465 with a database, "off" disables. Same parsing as the
  metrics addr; port must differ from the API's and the metrics' (port 0
  exempt). A host other than localhost or a loopback IP (empty, 0.0.0.0
  too) gets one WARN: plain HTTP. Anything but off without a database
  stops startup. Binds after
  metrics, before the API, logs "admin listening"; a later Serve error is
  ERROR and the API keeps serving.
- SENTINEL_ADMIN_TOKEN (required while the admin API is on):
  admin.ValidateToken; errors never hold it. Set while admin is off: one
  WARN, ignored.
- SENTINEL_ADDR (optional): listen address, default 127.0.0.1:8081
  (loopback only). Set it explicitly to expose the gateway on a network.
- SENTINEL_METRICS_ADDR (optional): metrics listener, default
  127.0.0.1:9464, serving only GET /metrics, never on the API port. "off"
  disables metrics (no listener, no Recorder). Must be host:port with a
  numeric port other than SENTINEL_ADDR's on any host (compared as
  numbers; port 0 exempt), else startup stops; so does a failed bind.
  Binds before the API listener and logs "metrics listening"; shut down
  after the API. A metrics Serve error later is logged at ERROR and the
  API keeps serving; API errors stay fatal. A provider panic is logged and
  recorded as a failure (500 unless a status was sent), then re-raised. The "unauthorized" WARN is rate-limited (one per
  10s, with a suppressed count) across all routes via one proxy.Auth.
- ANTHROPIC_BASE_URL (optional): upstream override, default https://api.anthropic.com.
  Must be https (plain http only for a loopback host); the gateway refuses
  to start otherwise. Base URL errors name at most the scheme and hostname,
  never the URL, which may hold a key. Other provider construction errors
  read "upstream <name>: ...".
- OPENAI_BASE_URL (optional): OpenAI-compatible upstream, default
  https://api.openai.com/v1. It includes /v1, like the OpenAI SDKs' own
  variable; the provider appends /chat/completions. Same https rule. For
  Ollama on the host: http://127.0.0.1:11434/v1.
- SENTINEL_MAX_CONCURRENT (optional): cap on requests in flight upstream,
  default 128. Also sizes the upstream idle connection pool and the Redis
  connection pool. Must be a
  positive integer; the gateway refuses to start otherwise.
- SENTINEL_UPSTREAM_CA_FILE (optional): PEM file of extra CA certificates
  trusted for every enabled upstream, added to the system pool (not
  replacing it). Every PEM block must be a valid certificate; the gateway
  refuses to start if the file is set but missing, holds none, or holds
  anything else. It warns for each enabled upstream left on its public
  default host (api.anthropic.com, api.openai.com) and when a cert is not
  a CA. It does not relax the https-only rule.
  `go run ./cmd/devcerts` writes a dev CA and server cert to ./certs
  (gitignored) for running cmd/mockupstream with -tls-cert/-tls-key.
- SENTINEL_REDIS_URL (optional): redis:// or rediss:// URL for per-key rate
  limiting. Unset means no rate limiting, logged at startup. A malformed URL
  stops startup (the error never repeats the URL, which may hold a
  password); an unreachable Redis at startup only warns.
- SENTINEL_RATE_LIMIT_RPM (optional): requests per minute per client key,
  default 60, also the burst. Must be a positive integer.
- SENTINEL_RATE_LIMIT_FAIL_MODE (optional): "open" (default) or "closed",
  for when Redis can't answer within 50 ms. Open lets the request through
  and logs a WARN at most every 10s; closed answers 503. Anything else stops
  startup. After 5 Redis failures in a row a circuit breaker skips Redis
  for 5s and applies the fail mode at once, then lets one request try
  again; opening and closing are logged. Only that probe can close it.
  When every Redis connection stays busy for the 50 ms, the request gets
  the fail mode but the breaker doesn't count it. The 50 ms is the call's
  context; new connections are dialed outside it (ratelimit.PoolDialTimeout
  2 s per attempt), and the pool keeps ratelimit.MinIdleConns (16, at most
  the pool size) open. At startup, after the ping, ratelimit.WarmUp sets
  them up (PING on each, at most 5 s): INFO "redis pool warm" ready=N, or
  WARN "redis pool warm-up incomplete" with ready and want, and startup
  goes on; a failed ping skips it. NewClient disables go-redis maint
  notifications (auto mode races across concurrent Conn handshakes). A client that disconnects
  during the check is logged as 499, not as a Redis failure.

Under Docker Compose the gateway reads ANTHROPIC_API_KEY, OPENAI_API_KEY,
OPENAI_BASE_URL, SENTINEL_UPSTREAM_<NAME>_*, SENTINEL_API_KEYS (make
up-real only), SENTINEL_ADMIN_TOKEN, SENTINEL_KEYS_REFRESH and SENTINEL_KEYS_MAX_STALE
from .env
(copy .env.example, which leaves the keys empty; .env is gitignored), and
SENTINEL_UPSTREAMS too, defaulting to anthropic. Compose sets SENTINEL_ADDR=0.0.0.0:8081 and
SENTINEL_REDIS_URL=redis://redis:6379/0 in the container and publishes the
port on 127.0.0.1 only. The gateway and gateway-bench get
SENTINEL_METRICS_ADDR=0.0.0.0:9464, unpublished; Prometheus (pinned image,
not published) scrapes both, so gateway-bench shows DOWN outside a bench.
Grafana (pinned image) is on 127.0.0.1:3000 with no admin account
(GF_SECURITY_DISABLE_INITIAL_ADMIN_CREATION), login form and basic auth
off, and no runtime plugin installs or updates (GF_PLUGINS_PREINSTALL_*):
with a read-only root, an update replacing the bundled Prometheus plugin
left Grafana without one. The dashboard queries the Prometheus
names (sentinel_requests_total, ..._seconds_bucket); a test in
internal/telemetry checks them against a real scrape. gateway-bench uses Redis database 1 with
SENTINEL_RATE_LIMIT_RPM=1000000, so the bench includes the Redis call but
never gets a 429. The mock override and gateway-bench set
SENTINEL_UPSTREAMS=anthropic,openai,mockcompat, ANTHROPIC_BASE_URL=
https://mockupstream:9090, OPENAI_BASE_URL=https://mockupstream:9090/v1,
mockcompat as protocol openai on https://mockupstream:9090/v1,
SENTINEL_UPSTREAM_CA_FILE=/certs/ca.pem and dummy keys for all three
(mock-not-a-real-key), so load tests never reach a real API. The mock
override also gives the gateway SENTINEL_DATABASE_URL on the postgres
service (postgres:18.6-alpine3.24, mock profile, no host port, volume
postgres-data, TCP pg_isready healthcheck the gateway waits on, hardened,
user postgres, dev-only password sentinel-dev-only in the compose file),
SENTINEL_API_KEYS="", SENTINEL_ADMIN_ADDR=0.0.0.0:9465 published on
127.0.0.1:9465 only, and SENTINEL_ADMIN_TOKEN=${SENTINEL_ADMIN_TOKEN:?} and
SENTINEL_KEYS_REFRESH=${SENTINEL_KEYS_REFRESH:-30s} and
SENTINEL_KEYS_MAX_STALE=${SENTINEL_KEYS_MAX_STALE:-10m}; the base gateway
blanks all three, so make up-real never gets the token. gateway-bench keeps
env keys. The gateway, gateway-bench and k6 mount only
certs/ca.pem; certs-init and mockupstream mount certs/ read-only. certs-init
copies the server key into a volume as a file owned by the nonroot user
(65532) with mode 0400, since devcerts writes it 0600 for the host user; the
volume itself stays root-owned. BENCH_API_KEY in .env (optional, default
"bench") is the only client key gateway-bench accepts.

## Git workflow
- Branch from dev. Open pull requests against dev, never main.
- main receives only milestone merges from dev.
- Merge dev into main with a merge commit, never squash.
