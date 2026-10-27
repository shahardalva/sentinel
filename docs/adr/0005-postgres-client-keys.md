# ADR 0005: Postgres for client keys

- Status: accepted
- Date: 2026-10-27

## Context

Client keys come from `SENTINEL_API_KEYS`: one list, fixed at startup, shared by everyone. Adding or revoking a key means editing the environment and restarting every gateway. There is no notion of who a key belongs to, so a key can't be revoked for one customer, or all of a customer's keys at once, and nothing per customer (limits, policies, audit) has anywhere to hang.

PR 1a put `proxy.Auth` behind a `proxy.KeySource` and added `keys.Refreshing`, which fetches SHA-256 digests on an interval and swaps them in. This ADR is about the store behind that fetch: `internal/store`, added in PR 1b. `cmd/gateway` doesn't use it yet; the configuration, the admin API and compose come in PR 1c.

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

`keys.Refreshing` calls `Store.ActiveDigests` every `Interval` (PR 1c chooses the default) and swaps the whole set in. Consequences:

- **Revocation latency is about `Interval` plus the fetch time.** A key revoked just after a fetch read its rows is still accepted until the next fetch, which starts `Interval` after that one returns, finishes. The worst case is `Interval` plus two fetch times, bounded by `Interval + 2 × FetchTimeout`. A request already authenticated is not affected: a stream that began before the revoke runs to its end.
- **While the database is unreachable, the last good set stays.** Revoked keys keep working and new keys don't work until a refresh succeeds. That is the price of not making Postgres a hard dependency of every request; `sentinel_auth_key_refresh_age_seconds` shows it, and is what to alert on (a `Fetch` that hangs records no failure).
- **The first fetch must succeed** within `FetchTimeout`, or the gateway doesn't start: it never serves on a set it hasn't loaded. A first fetch that succeeds with **no keys is accepted**: the gateway starts and rejects every request, with one `WARN`. Refusing to start would turn "every key revoked" into "gateway down", and a gateway that starts before its first tenant exists is how a fresh deployment begins.
- **One key source per process**, so the key metrics have no source label (`proxy.Recorder`). PR 1c chooses between `SENTINEL_API_KEYS` and Postgres; it does not merge them.

### Error sanitizing

pgx's errors carry connection details: a connect error reads ``failed to connect to `user=... database=...`: host:port ...``, a parse error quotes the connection string with only the password masked, and the server's own messages name the user or database (`password authentication failed for user "..."`). `keys.Refreshing` logs fetch errors as they are, and PR 1c will log `Open`'s.

So no error the store returns holds the connection string, user, password, host or database name. Every pgx error becomes a `*store.Error` whose message is the operation, a fixed description ("cannot connect to postgres", "timed out", "postgres error"...), and for a server error its SQLSTATE code and constraint name, which come from this package's migrations. pgx's message is dropped, not wrapped, so nothing that walks the error chain can print it; `Unwrap` returns only `context.Canceled` or `context.DeadlineExceeded`, so callers can still tell a timeout. The typed errors `ErrNotFound`, `ErrDuplicateName`, `ErrTenantDisabled`, `ErrInvalidName`, `ErrSchemaTooNew` and `ErrSchemaMismatch` are for `errors.Is`. Only an `*Error` itself or a bare sentinel passes through the sanitizer unchanged; an error that merely wraps one is sanitized like any other, since its wrapper's text could hold anything. Tests connect to `127.0.0.1:1` and, with a real Postgres, with a wrong password, using a distinctive user, password and database name, and check that none of them, nor the host, appears.

The cost is debuggability: "postgres error (SQLSTATE 42P01)" says less than the server's message. The code is enough to look up, and the operator can read Postgres's own log.

pgx also reads the libpq environment variables (`PGHOST`, `PGPASSWORD`...) and `~/.pgpass` for anything the connection string leaves out; that is where `pgpassfile` and `pgservicefile` below come from.

### How #1 and #24 fit

Neither gets a column now: a column nothing reads or enforces would look like a feature that isn't there.

- **#1, per-client concurrency limits.** A later migration adds a nullable `max_concurrent` to `api_keys` (or `tenants`, if the limit is per customer). `ActiveDigests` would return each digest with its limit, `KeySet` would carry it beside the digest, and `Auth` would put the limit in the request context with the client ID for the admission stage to enforce. The refresh model already covers changing a limit without a restart.
- **#24, an HMAC rate-limit bucket key.** The bucket ID becomes `HMAC-SHA256(secret, key)`, computed by the gateway from the presented key, which it has on every request. That needs no schema change: the store never sees the bucket ID. With the store there is a second option, once `KeySet` maps a digest to its `api_keys.id`: use that UUID as the bucket ID. It reveals nothing about the key at all and needs no secret, but works only for store-issued keys, so `SENTINEL_API_KEYS` would still need the HMAC.

### The constant-time loop vs a map lookup

`proxy.Auth` compares the presented key's digest against every digest with `subtle.ConstantTimeCompare` and doesn't stop at a match. That is O(N) per request. A throwaway benchmark on the bench machine (i7-7700) measured about 20 ns per digest: 0.2 µs for 10 keys, 19 µs for 1,000, 0.2 ms for 10,000 and 2 ms for 100,000, against about 10 to 30 ns for a map lookup at any of those sizes.

A `map[[32]byte]` lookup's timing depends on the digest, which an attacker chooses only through SHA-256 of the key they send. Learning anything about a stored digest from map timing would need control over the digest, which means finding SHA-256 preimages, and even a full stored digest is useless without the 256-bit key behind it. So a map is safe here, for the same reason SHA-256 is enough above.

The loop stays for now: PR 1a's `Auth` is unchanged and tested, and at the expected scale (tens to a few thousand keys) it costs microseconds. It switches to a map, built once per refresh in `NewKeySet`, before key counts reach the tens of thousands, where it becomes a measurable share of the gateway's overhead (about 1 ms at p50, bench/RESULTS.md). The switch is local to `KeySet` and `Auth`.

### Modules added

Direct: `github.com/jackc/pgx/v5` v5.11.0. Indirect: `github.com/jackc/puddle/v2` (pgxpool's resource pool), `github.com/jackc/pgpassfile` and `github.com/jackc/pgservicefile` (libpq's `.pgpass` and service file), `golang.org/x/sync` and `golang.org/x/text`. All are maintained by pgx's author or the Go project. The binary size is measured in PR 1c, when `cmd/gateway` imports the store; until then nothing links it.

### Testing

Unit tests need no database: the sanitizing (an unreachable server, bad connection strings, a server error with names in its message), the key format, and digest equality with `proxy.DigestKeys`. The rest run on a real Postgres when `SENTINEL_TEST_POSTGRES_URL` is set, each in a schema of its own: migrating twice, five gateways with separate pools migrating at once, the same under a serializable default, a failed migration applying nothing, the newer-version and name-mismatch refusals, an up-to-date migrate without CREATE, the digest length check, create, revoke and disable against `ActiveDigests`, `CreateKey` waiting for a concurrent disable, the typed errors, and end to end with `keys.Refreshing` and `proxy.Auth`, where a revoked key is rejected after one refresh. `make test-postgres` runs them in a throwaway `postgres:18.6-alpine3.24` on `127.0.0.1:5433`, and CI runs them against the same image as a service.

## Consequences

- A second stateful service to run in production once PR 1c lands, and the first whose loss matters for keys: the gateway can't start without it, and runs on its last good key set while it is down.
- Revocation takes up to a refresh interval; anyone needing a key dead at once still has to restart the gateways.
- Five more modules in `go.mod`.
- Schema changes are forward-only migrations in this repository, applied by the gateway at startup (PR 1c decides whether that is automatic or a separate command).
- `SENTINEL_API_KEYS` stays for development and small deployments.
