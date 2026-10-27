# ADR 0005: Postgres for client keys

- Status: accepted
- Date: 2026-10-27

## Context

Client keys come from `SENTINEL_API_KEYS`: one list, fixed at startup, shared by everyone. Adding or revoking a key means editing the environment and restarting every gateway. There is no notion of who a key belongs to, so a key can't be revoked for one customer, or all of a customer's keys at once, and nothing per customer (limits, policies, audit) has anywhere to hang.

PR 1a put `proxy.Auth` behind a `proxy.KeySource` and added `keys.Refreshing`, which fetches SHA-256 digests on an interval and swaps them in. This ADR is about the store behind that fetch: `internal/store`, added in PR 1b. The admin API is PR 1c (a package, below), and PR 1d wires the store, the refresh and the admin API into `cmd/gateway` and adds Postgres to the mock compose stack (Wiring, below).

## Decision

### Postgres

Tenants and keys are relational, small and read far more than written: a row per tenant, a row per key, a foreign key between them, unique names and digests, and a query that joins them. They need durability and transactions, not throughput. Postgres is the database the roadmap already names for the platform's state (tenants, policies, the audit index), so this is its first use, not a database chosen for keys alone.

Redis already runs, but it is a cache with optional persistence, in the deployment and in ADR 0002, and the rate limiter fails open when it is gone. Losing keys must not be a mode of operation. Redis also has no constraints: a duplicate tenant name or a key for a missing tenant would be the application's problem.

The hot path doesn't touch Postgres. `proxy.Auth` reads the current `KeySet` from memory with one atomic load; only the refresh loop queries the database, once per interval, for the whole set.

### pgx, without sqlc for now

`github.com/jackc/pgx/v5` with its pool, `pgxpool`, used directly. pgx is the maintained native Postgres driver for Go: it speaks the protocol itself rather than through `database/sql`, supports context cancellation on every call, and has typed errors (`*pgconn.PgError` with the SQLSTATE code and constraint name) that the typed errors below are built on.

The earlier plan (docs/notes.md) named sqlc for queries. The store has seven queries, all short; generating code for them would add a code generator to the toolchain and generated files to review, for less code than it produces. The queries are hand-written with pgx. sqlc is worth revisiting when the admin API and policies bring enough queries that a schema/query mismatch becomes a real risk.

### Own migrations, not a library

Migrations are embedded `.sql` files (`internal/store/migrations/NNNN_name.sql`, versions 1, 2, 3... with no gap, checked by a test) and a runner of about 100 lines:

- A `schema_migrations` table (version, name, applied_at).
- Each migration runs in its own transaction, which first takes `pg_advisory_xact_lock` on a fixed key, then reads the applied migrations and applies the next one, if any, and records it. Gateways that start at once serialize on the lock; each migration is applied once, and the lock is released by the commit or rollback, so a gateway that dies mid-migration leaves no lock behind. A migration that fails halfway is rolled back whole: the database stays at the version before it.
- The transaction is always **READ COMMITTED**, whatever the server's, role's or connection's `default_transaction_isolation`. The runner depends on it: each statement after the lock must see what the gateway that held the lock before it committed. Under REPEATABLE READ or SERIALIZABLE the snapshot is taken by the lock statement, before the wait, so a gateway that waited would read the old version and run the same migration again, failing on `CREATE TABLE` (or with a serialization error) and not starting.
- `schema_migrations` is created only when `to_regclass` says it is missing, not with `CREATE TABLE IF NOT EXISTS`, which needs the CREATE privilege on the schema even when the table exists. An up-to-date database needs only USAGE on the schema and SELECT on `schema_migrations`, so a gateway can run as a role that can't change the schema.
- A database at a version newer than the binary knows is `ErrSchemaTooNew`, and nothing is changed. An older gateway rolled back onto a newer schema must not run against tables it doesn't understand. An applied migration whose name differs from the binary's at that version (one from another branch, say) is `ErrSchemaMismatch`, and nothing is changed either.

The earlier plan named goose. goose and golang-migrate do all of this and more, but bring their own dependency trees, CLI-oriented behavior, and support for databases and source drivers the gateway will never use. What the gateway needs is small enough to read in one sitting, and the behaviors that matter for a security component are each covered by a test against a real Postgres: five gateways migrating an empty database at once (also with `default_transaction_isolation=serializable`), a migration that fails halfway leaving the database at the previous version with none of its statements applied, the newer-version and name-mismatch refusals, and an up-to-date database migrated by a role without CREATE. Down migrations are left out on purpose: rolling back is a new forward migration.

### Schema

Migration 0001:

- `tenants`: `id uuid` (default `gen_random_uuid()`), `name text not null unique`, `created_at timestamptz not null default now()`, `disabled_at timestamptz null`.
- `api_keys`: `id uuid`, `tenant_id uuid not null` referencing `tenants`, `prefix text not null`, `digest bytea not null unique` with a check that it is exactly 32 bytes, `created_at`, `revoked_at timestamptz null`. An index on `tenant_id` serves `ListKeys` and the foreign key.

Nothing is ever deleted: disabling a tenant and revoking a key set a timestamp, so the audit log can always name what a past request used. `ActiveDigests` returns the digests of keys not revoked whose tenant is not disabled.

**UUIDs** for IDs, not serial integers: they don't reveal how many tenants or keys exist or in what order they were made, can't be guessed from a neighbor's ID in an admin API, and can be generated anywhere without a round trip. `gen_random_uuid()` is built into Postgres 13 and later. A malformed ID passed to the store is `ErrNotFound`, checked before any query.

### Key format

`snl_` followed by 32 bytes from `crypto/rand` in unpadded base64url: 47 characters, 256 bits of entropy. The prefix makes a leaked key recognizable to secret scanners and people, and says which system issued it. `prefix`, the first 12 characters (`snl_` and 8 more, 48 bits), is stored in the clear to tell keys apart in listings; it is far too short to help guess the rest.

The plaintext key is returned once by `CreateKey` and never stored or logged; the store keeps only its digest. The store never logs anything at all.

### SHA-256, not a slow hash

The digest is SHA-256 of the full key, the same as `proxy.DigestKeys`, so `proxy.Auth` accepts a store-issued key unchanged; a test checks they agree. Password hashes (bcrypt, scrypt, Argon2) are slow on purpose, so that guessing a *low-entropy* secret from a stolen hash costs years. These keys are not low-entropy: a 256-bit random key can't be guessed from its digest at any hash speed, so a slow hash would buy nothing. It would cost a lot: the gateway checks a key on every request, and a slow hash per request (tens of milliseconds, by design) is both the hot path's whole budget and a CPU denial-of-service lever for anyone sending bad keys. This holds only because the store generates the keys. Keys from `SENTINEL_API_KEYS` are whatever the operator chose, which is why ADR 0002 recommends long random ones.

### The refresh model

`keys.Refreshing` calls `Store.ActiveDigests` every `Interval` (`SENTINEL_KEYS_REFRESH`, default 30 s, at least 1 s) and swaps the whole set in. Consequences:

- **Revocation latency is about `Interval` plus the fetch time.** A key revoked just after a fetch read its rows is still accepted until the next fetch, which starts `Interval` after that one returns, finishes. The worst case is `Interval` plus two fetch times, bounded by `Interval + 2 × FetchTimeout`. A request already authenticated is not affected: a stream that began before the revoke runs to its end.
- **While the database is unreachable, the last good set stays.** Revoked keys keep working and new keys don't work until a refresh succeeds. That is the price of not making Postgres a hard dependency of every request; `sentinel_auth_key_refresh_age_seconds` shows it, and is what to alert on (a `Fetch` that hangs records no failure).
- **The first fetch must succeed** within `FetchTimeout`, or the gateway doesn't start: it never serves on a set it hasn't loaded. A first fetch that succeeds with **no keys is accepted**: the gateway starts and rejects every request, with one `WARN`. Refusing to start would turn "every key revoked" into "gateway down", and a gateway that starts before its first tenant exists is how a fresh deployment begins.
- **One key source per process**, so the key metrics have no source label (`proxy.Recorder`). The gateway takes keys from `SENTINEL_API_KEYS` or from Postgres, never both: with `SENTINEL_DATABASE_URL` set, a non-empty `SENTINEL_API_KEYS` stops startup.

### Error sanitizing

pgx's errors carry connection details: a connect error reads ``failed to connect to `user=... database=...`: host:port ...``, a parse error quotes the connection string with only the password masked, and the server's own messages name the user or database (`password authentication failed for user "..."`). `keys.Refreshing` logs fetch errors as they are, and the gateway logs `Open`'s and `Migrate`'s when they stop startup.

So no error the store returns holds the connection string, user, password, host or database name. Every pgx error becomes a `*store.Error` whose message is the operation, a fixed description ("cannot connect to postgres", "timed out", "postgres error"...), and for a server error its SQLSTATE code and constraint name, which come from this package's migrations. pgx's message is dropped, not wrapped, so nothing that walks the error chain can print it; `Unwrap` returns only `context.Canceled` or `context.DeadlineExceeded`, so callers can still tell a timeout. The typed errors `ErrNotFound`, `ErrDuplicateName`, `ErrTenantDisabled`, `ErrInvalidName`, `ErrSchemaTooNew` and `ErrSchemaMismatch` are for `errors.Is`. Only an `*Error` itself or a bare sentinel passes through the sanitizer unchanged; an error that merely wraps one is sanitized like any other, since its wrapper's text could hold anything. Tests connect to `127.0.0.1:1` and, with a real Postgres, with a wrong password, using a distinctive user, password and database name, and check that none of them, nor the host, appears.

The cost is debuggability: "postgres error (SQLSTATE 42P01)" says less than the server's message. The code is enough to look up, and the operator can read Postgres's own log.

pgx also reads the libpq environment variables (`PGHOST`, `PGPASSWORD`...) and `~/.pgpass` for anything the connection string leaves out; that is where `pgpassfile` and `pgservicefile` below come from.

### Admin API

`internal/admin` (PR 1c) is the HTTP API over the store: `POST`/`GET /admin/tenants`, `POST /admin/tenants/{id}/disable`, `POST`/`GET /admin/tenants/{id}/keys` and `DELETE /admin/keys/{id}`, JSON in and out, one error shape (`{"error":{"type","message"}}`) for every failure including 404 and 405. It is a package returning an `http.Handler`; PR 1d gives it a listener (`SENTINEL_ADMIN_ADDR`).

- **Its own listener, never the API port.** It is served on a separate address, `SENTINEL_ADMIN_ADDR` (default `127.0.0.1:9465`, `off` to disable, an error without a database), the same pattern as `SENTINEL_METRICS_ADDR`: refused if it shares the API's port or the metrics port, so no client of the gateway can reach it by path, and an operator can firewall it on its own. Nothing about the admin routes is registered on the API mux.
- **A bearer token**, `Authorization: Bearer <token>`, checked before anything else: before routing, so without it every path and method gets the same 401 (`WWW-Authenticate: Bearer`, the same body) and nothing reveals which routes exist; and before the body is read, so an unauthenticated client can't make the gateway read or parse anything. Only the token's SHA-256 digest is kept, and the presented token's digest is compared in constant time, so timing reveals neither length nor contents. `ValidateToken` (checked at startup on `SENTINEL_ADMIN_TOKEN`) requires at least 32 characters of printable ASCII, no space: no whitespace or control character that a shell, a `.env` file or a client could send differently from how it was configured. net/http trims whitespace around a header value, so `Authorization:  Bearer <token>  ` is accepted; whitespace inside it (two spaces, a tab) is not. Rejections are logged at WARN at most once per 10 s with a count of the ones held back, as `proxy.Auth` does. The client chooses the path, up to the server's header limit, so the warning logs it escaped (all ASCII) and cut at 256 bytes, with `path_truncated=true` and `path_len` when cut.
- **Browsers can't use it without the token.** It has no cookies or other ambient credential, so a cross-site request (CSRF) from a page the operator visits carries nothing that authenticates it. A page can't add an `Authorization` header to a cross-origin request without a CORS preflight, which the API never answers with CORS headers: a preflight carries no token, so it gets the 401 (an `OPTIONS` with the token gets a 405), and the browser doesn't send the request. One request never reaches the handler: `OPTIONS *` is answered by net/http itself, with a 200 and none of the headers below. It reveals nothing, but the admin listener's server sets `http.Server{DisableGeneralOptionsHandler: true}` on the admin listener so that it too gets the handler's 401. DNS rebinding makes the page same-origin with the admin listener, which lifts the CORS barrier, but the page still doesn't have the token. A simple form POST also fails the body rules (`application/json` only, 415 otherwise), though the 401 comes first.
- **The plaintext key** appears in exactly one response, the 201 to `POST /admin/tenants/{id}/keys`, and never in a list or a log line. Every response is `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`.
- **Input**: a 4 KiB body limit (413). A non-empty body must be `application/json` (UTF-8 or no charset; 415), with unknown fields and anything after the first JSON value rejected (400); an empty or all-whitespace body is no body, whatever its `Content-Type` or framing (an empty chunked body included). `DELETE` takes no body (400). encoding/json matches field names ignoring case (`{"NAME":"x"}` is a name) and lets a repeated field's last value win; with one field, which the store validates, that is accepted rather than worked around with a stricter decoder. A path that isn't clean is a 400, checked on the escaped path as ServeMux matches it: an escaped `/` or `.` (`%2F`, `%2e`) is part of an ID, so `/admin/keys/a%2Fb` reaches the store as the ID `a/b`, which isn't a UUID, so not found.
- **Errors**: the store's typed errors map to 400 (`ErrInvalidName`), 404 (`ErrNotFound`, also for a malformed ID), 409 (`ErrDuplicateName`, `ErrTenantDisabled`); anything else is a 500 that says only "internal error", while the store's sanitized error goes to the log at ERROR. A read the client canceled by going away (`context.Canceled` while the request's context is done) gets nothing written and is logged at WARN as 499, as the gateway does. A change can't be canceled so, so any error from it is the store's: ERROR and a 500, even if the client has gone. A store call that hits the 5 s deadline is an ERROR and a 500 too.
- **Store deadlines**: every store call has 5 s. A read also ends when the client disconnects. A change doesn't (`context.WithoutCancel`): once it has started, a client that disconnects can't cancel it after Postgres has committed it, which would leave a committed change logged as a failure and without its audit line.
- **Audit**: each successful change logs one INFO line, `msg=admin`, with the action (`create_tenant`, `disable_tenant`, `create_key`, `revoke_key`), the tenant ID, key ID and key prefix where known, the remote address, and `actor=admin-token`. Never the key or the token. Reads aren't logged. These are log lines until the audit log exists, and go to it then.

Known limits, on purpose for now:

- **One shared secret.** Every operator holds the same token, so the audit log's actor can say only "admin-token", not who; rotating it means changing it everywhere and restarting; and anyone with it can do everything, including issuing keys for any tenant. Per-operator credentials (or mTLS, or an identity provider) come when there is more than one operator to tell apart.
- **No rate limit on the admin API.** A long random token (32 characters is the floor, not a recommendation; generate it, for example with `openssl rand -base64 32`) can't be guessed at any request rate, and the throttled WARN keeps a flood of bad tokens from flooding the log, but nothing slows down a client that has the token, or bounds the store calls it causes. The listener is meant to be reachable only by operators.
- No TLS of its own: on loopback, or behind something that terminates TLS.

### Wiring (PR 1d)

`SENTINEL_DATABASE_URL` set selects the database; unset or empty keeps `SENTINEL_API_KEYS`. A blank value (only spaces) stops startup rather than meaning "unset": pgx reads an empty connection string as "all defaults", from the `PG*` variables and `~/.pgpass`, which is not what a blank variable should mean. No configuration error or log line holds the URL.

At startup, in order, before any listener binds:

1. **`store.Open`, with a 10 s deadline.** A TCP connect, TLS if configured, authentication and a ping. Without a deadline, a database behind a firewall that drops packets holds startup for the operating system's connect timeout, which is minutes. 10 s is far beyond a healthy connect, even across regions or to a database that just started, and matches the longest default fetch timeout of `keys.Refreshing`.
2. **`Migrate`, with a 30 s deadline**, automatically, on every start (then `SchemaVersion`, for the startup log, under the same deadline). The migrations so far are a few `CREATE TABLE` statements that take milliseconds; most of the time is waiting on the advisory lock while other gateways starting at once migrate in turn. 30 s covers that with room to spare. A migration that needs longer, such as a backfill on a big table, should be run on purpose rather than on a gateway's start, and the deadline rolls it back whole instead of leaving startup hanging.
3. **`keys.NewRefreshing(store.ActiveDigests)`**, with the metrics Recorder. Its first fetch has its own timeout, `min(Interval/2, 10 s)`: 10 s at the default 30 s.

Each failure stops startup with an error that names the step and carries the store's sanitized error; one that ran out of its own deadline says which (`startup deadline 10s`). A signal during any of them stops startup too. The deadlines are not configurable: they bound a failure, they don't tune a healthy start.

The refresh runs on a context of its own, not the signal's, so keys keep refreshing while the API drains its requests at shutdown. Shutdown goes: the API (30 s grace), then the admin server (5 s), then metrics, then the refresh is cancelled and waited for (5 s; a fetch honors its context, so it returns at once), and only then is the pool closed, after everything that uses it has stopped. If the refresh hasn't stopped in 5 s the pool is left open, since `Close` would wait for the stuck fetch's connection.

The admin listener binds after the metrics one and before the API, so the API's `listening` line still means the whole gateway is up; it logs `admin listening`. Its `http.Server` has whole-request timeouts like the metrics one (5 s headers, 10 s read, 15 s write, 60 s idle): every admin request is short, with a 5 s store call. A bind failure stops startup. An admin server that fails once serving is logged at ERROR and the API keeps serving, as with metrics. The keys already loaded keep working and keep refreshing, so losing the admin API means an operator can't issue or revoke keys until a restart: a management outage. Taking the API down with it would turn that into an outage for every client.

`SENTINEL_KEYS_REFRESH` must be at least 1 s. The fetch timeout is half the interval, and under a second it gets too short for a round trip to a database over a network; and a typo such as `30ms` would otherwise query the database 33 times a second per gateway. Without a database, `SENTINEL_KEYS_REFRESH` and `SENTINEL_ADMIN_TOKEN` are ignored with a WARN, as an unused upstream key is; a `SENTINEL_ADMIN_ADDR` other than `off` is an error, since it asks for something that can't exist.

The mock compose stack (`make up`) runs `postgres:18.6-alpine3.24` in the `mock` profile: no host port, a named volume, a TCP healthcheck the gateway waits on, the shared hardening (read-only root, every capability dropped, `no-new-privileges`), and the image's `postgres` user, so the entrypoint needs no capability. Its password is a dev-only default written in the compose file. The admin token has no default (`${SENTINEL_ADMIN_TOKEN:?}`). `gateway-bench` keeps keys from the environment.

### How #1 and #24 fit

Neither gets a column now: a column nothing reads or enforces would look like a feature that isn't there.

- **#1, per-client concurrency limits.** A later migration adds a nullable `max_concurrent` to `api_keys` (or `tenants`, if the limit is per customer). `ActiveDigests` would return each digest with its limit, `KeySet` would carry it beside the digest, and `Auth` would put the limit in the request context with the client ID for the admission stage to enforce. The refresh model already covers changing a limit without a restart.
- **#24, an HMAC rate-limit bucket key.** The bucket ID becomes `HMAC-SHA256(secret, key)`, computed by the gateway from the presented key, which it has on every request. That needs no schema change: the store never sees the bucket ID. With the store there is a second option, once `KeySet` maps a digest to its `api_keys.id`: use that UUID as the bucket ID. It reveals nothing about the key at all and needs no secret, but works only for store-issued keys, so `SENTINEL_API_KEYS` would still need the HMAC.

### The constant-time loop vs a map lookup

`proxy.Auth` compares the presented key's digest against every digest with `subtle.ConstantTimeCompare` and doesn't stop at a match. That is O(N) per request. A throwaway benchmark on the bench machine (i7-7700) measured about 20 ns per digest: 0.2 µs for 10 keys, 19 µs for 1,000, 0.2 ms for 10,000 and 2 ms for 100,000, against about 10 to 30 ns for a map lookup at any of those sizes.

A `map[[32]byte]` lookup's timing depends on the digest, which an attacker chooses only through SHA-256 of the key they send. Learning anything about a stored digest from map timing would need control over the digest, which means finding SHA-256 preimages, and even a full stored digest is useless without the 256-bit key behind it. So a map is safe here, for the same reason SHA-256 is enough above.

The loop stays for now: PR 1a's `Auth` is unchanged and tested, and at the expected scale (tens to a few thousand keys) it costs microseconds. It switches to a map, built once per refresh in `NewKeySet`, before key counts reach the tens of thousands, where it becomes a measurable share of the gateway's overhead (about 1 ms at p50, bench/RESULTS.md). The switch is local to `KeySet` and `Auth`.

### Modules added

Direct: `github.com/jackc/pgx/v5` v5.11.0. Indirect: `github.com/jackc/puddle/v2` (pgxpool's resource pool), `github.com/jackc/pgpassfile` and `github.com/jackc/pgservicefile` (libpq's `.pgpass` and service file), `golang.org/x/sync` and `golang.org/x/text`. All are maintained by pgx's author or the Go project.

Binary size of `cmd/gateway`, Go 1.27.0, measured in PR 1d against `dev` at 2847d59, the commit before it:

| Build | Before | After | Change |
| --- | --- | --- | --- |
| linux/amd64, `CGO_ENABLED=0 -trimpath -ldflags="-s -w"` (the Dockerfile's) | 13,082,784 B (12.5 MiB) | 17,281,184 B (16.5 MiB) | +4.2 MB, +32% |
| windows/amd64, plain `go build` | 19,361,792 B | 25,406,976 B | +6.0 MB, +31% |

Of the stripped build's 4.2 MB, pgx's own symbols are about 1.2 MB and `golang.org/x/text` (pgx's text encodings) about 0.4 MB; `internal/store` and `internal/admin` together about 33 KB. The rest is the runtime metadata that comes with them (type data, the PC tables) and standard library packages the gateway didn't link before. The binary holds 29 modules against 23. A third more binary is a real cost for a gateway that prides itself on few dependencies; it buys a maintained driver instead of a hand-rolled one, and it is a cost to weigh again before adding a second database driver.

### Testing

Unit tests need no database: the sanitizing (an unreachable server, bad connection strings, a server error with names in its message), the key format, and digest equality with `proxy.DigestKeys`. The rest run on a real Postgres when `SENTINEL_TEST_POSTGRES_URL` is set, each in a schema of its own: migrating twice, five gateways with separate pools migrating at once, the same under a serializable default, a failed migration applying nothing, the newer-version and name-mismatch refusals, an up-to-date migrate without CREATE, the digest length check, create, revoke and disable against `ActiveDigests`, `CreateKey` waiting for a concurrent disable, the typed errors, and end to end with `keys.Refreshing` and `proxy.Auth`, where a revoked key is rejected after one refresh. `make test-postgres` runs them in a throwaway `postgres:18.6-alpine3.24` on `127.0.0.1:5433`, and CI runs them against the same image as a service.

## Consequences

- A second stateful service to run in production, and the first whose loss matters for keys: the gateway can't start without it, and runs on its last good key set while it is down.
- Revocation takes up to a refresh interval; anyone needing a key dead at once still has to restart the gateways.
- Five more modules in `go.mod`.
- Schema changes are forward-only migrations in this repository, applied automatically by every gateway at startup. The gateway's role must be able to change the schema; a separate migrate command and a least-privilege runtime role are left for later.
- The gateway binary is about a third larger (Modules added).
- `SENTINEL_API_KEYS` stays for development and small deployments.
