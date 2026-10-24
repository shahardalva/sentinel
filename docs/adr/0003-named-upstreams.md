# ADR 0003: Named upstreams

- Status: accepted
- Date: 2026-10-24

## Context

The gateway speaks two protocols, the Anthropic Messages API and the OpenAI Chat Completions API. Until now each protocol had exactly one upstream: `ANTHROPIC_BASE_URL` and `OPENAI_BASE_URL` each named a single server. Most of the providers people want in front of an LLM gateway speak one of those two protocols: xAI, Groq, Together, Mistral, DeepSeek, OpenRouter, and local servers such as Ollama and vLLM speak OpenAI's, and some proxies and hosted models speak Anthropic's. Serving OpenAI and a local Ollama side by side, or two OpenAI-compatible providers, meant running two gateways.

The goal is that any such provider is added by configuration alone, with no code change, and without weakening the rules the built-in upstreams follow: https only (plain http only for loopback), keys that never appear in errors or logs, and one shared concurrency cap ([ADR 0001](0001-concurrency-cap.md)).

## Decision

### Protocol and upstream are separate

A **protocol** is a wire format and the code that speaks it: the route's path, the request and response header rules, and the error shape. There are two, `anthropic` and `openai`, each a `Provider` implementation. An **upstream** is a named server that speaks one protocol, with its own base URL and key. Many upstreams can share a protocol.

The request log keeps `provider` as the protocol's `Name()` and adds `upstream`, the upstream's name, so traffic can be split by either.

### An explicit list in SENTINEL_UPSTREAMS

`SENTINEL_UPSTREAMS` lists every upstream to serve, built-in and custom. Nothing is enabled because a variable happens to be set: an upstream not in the list is never contacted, and its variables are neither validated nor used. The gateway logs a `WARN` for each unlisted upstream whose variables are set, once per name and without values, so a leftover variable is visible rather than silently ignored or silently used.

### Names and variables

A name matches `^[a-z][a-z0-9]{0,31}$`. A custom upstream `NAME` is configured by three variables, with `NAME` upper-cased:

- `SENTINEL_UPSTREAM_<NAME>_PROTOCOL`: `anthropic` or `openai`
- `SENTINEL_UPSTREAM_<NAME>_BASE_URL`
- `SENTINEL_UPSTREAM_<NAME>_API_KEY`

There are no hyphens or underscores in names, so a variable maps back to exactly one name and the scan for unlisted upstreams is unambiguous (`SENTINEL_UPSTREAM_CA_FILE` is not an upstream's, since `FILE` is not one of the three fields). `v1` is rejected, since `/v1/v1/...` routes would be confusing next to the unprefixed `/v1/...` ones.

The built-in names `anthropic` and `openai` keep `ANTHROPIC_*` and `OPENAI_*`, so existing setups don't change. Setting any `SENTINEL_UPSTREAM_ANTHROPIC_*` or `SENTINEL_UPSTREAM_OPENAI_*` variable stops startup with an error naming the variables the built-in uses. Two ways to configure one upstream would make it unclear which one won.

### No defaults for custom upstreams

All three variables are required. A built-in upstream has a known public default; a custom one doesn't, and guessing one (OpenAI's, say) would send a key meant for one provider to another. Validation is the built-ins': the base URL rule from `internal/provider/upstream`, the key checks for edge whitespace and control characters, and errors that name the variable at fault but never print a URL or a key.

### Prefix routing

Every enabled upstream gets `POST /<name>/v1/messages` or `POST /<name>/v1/chat/completions`, by its protocol. The unprefixed `/v1/messages` and `/v1/chat/completions` stay, served by the built-in `anthropic` and `openai` when they are enabled. A client chooses an upstream through its SDK's base URL, `http://gateway:8081/xai/v1`, which every SDK already supports.

The alternative was routing by the request's `model` field. It needs no client change, but the model field is client-controlled and per request, so the gateway would have to keep a model-to-upstream table current across providers that rename and add models all the time, and decide what an unknown model means. The path is fixed at configuration time, visible in the URL, and fails closed: an unknown prefix is a `404`. Routing by model can be added later on top of names.

All routes share the client keys, the rate limiter and one `proxy.Admission`, so the memory bound in ADR 0001 holds however many upstreams there are.

### Environment variables, not a config file, for now

Upstreams are configured like everything else, from the environment. A config file would be one more format to parse and validate and one more place for keys to end up on disk. The long-term home for upstreams is Postgres, per tenant, with keys in a secret store; a file format designed now would be replaced then.

### The bench guard checks resolved configuration

`make bench-gateway` used to check that `gateway-bench` had the two base URL variables set to the mock. A check of variables has to know every upstream in advance, and a built-in upstream added to `SENTINEL_UPSTREAMS` without its base URL silently gets its public default, which such a check doesn't see. Now the guard waits for `gateway-bench` to log `listening` and reads its `upstream enabled` lines, which carry the base URL the gateway will actually call. It refuses to run k6 unless there is at least one, and every one is `https://mockupstream:9090` or a path under it, naming the upstream at fault otherwise. The matching is in `cmd/benchguard`, which has unit tests.

## Consequences

- Any OpenAI- or Anthropic-compatible provider is a few variables, with no code change.
- Several upstreams of one protocol can run side by side, each with its own key; a key reaches only its own upstream.
- Clients of a custom upstream change their base URL to include the prefix. Clients of the built-ins don't change.
- The startup log has one `upstream enabled` line per upstream with its protocol and base URL, and the request log names the upstream.
- `loadConfig` takes the environment list as well as `getenv`, so tests stay hermetic.

## Deferred

- **Per-upstream CA.** `SENTINEL_UPSTREAM_CA_FILE` still applies to every enabled upstream. A private CA for a local vLLM is then also trusted for the public providers. The default-host warning covers only the built-ins.
- **Per-upstream rate limits.** The limit is per client key and shared across upstreams.
- **Routing by model**, and fallback between upstreams.
- **Azure OpenAI, Amazon Bedrock and Google Vertex AI.** They differ in authentication (Azure `api-key` headers and deployments, SigV4, Google OAuth), not just in base URL, so they need providers of their own.
