COMPOSE         := docker compose
COMPOSE_MOCK    := $(COMPOSE) -f docker-compose.yml -f docker-compose.mock.yml --profile mock
COMPOSE_BENCH   := $(COMPOSE_MOCK) --profile bench
MOCK_URL        := https://mockupstream:9090

.DEFAULT_GOAL := help
.PHONY: help fmt vet test test-redis certs run up up-real down logs bench bench-gateway

help: ## List the targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format Go code with gofmt
	gofmt -w .

vet: ## Run go vet
	go vet ./...

test: ## Run tests with the race detector (needs cgo and a C compiler)
	go test -race ./...

# A throwaway redis:7-alpine on 127.0.0.1:6390, removed afterwards.
test-redis: ## Run the tests with the race detector, the rate-limit ones also on a real Redis (needs Docker)
	docker run -d --rm --name sentinel-test-redis -p 127.0.0.1:6390:6379 redis:7-alpine
	SENTINEL_TEST_REDIS_URL=redis://127.0.0.1:6390/0 go test -race ./...; status=$$?; 	docker stop sentinel-test-redis >/dev/null; exit $$status

certs: ## Write a dev CA and server cert to ./certs if missing or expiring within 7 days
	go run ./cmd/devcerts -hosts localhost,127.0.0.1,mockupstream -renew-within 168h

run: ## Run the gateway on the host (needs the env vars from CLAUDE.md)
	go run ./cmd/gateway

# --force-recreate so mockupstream and the gateway pick up renewed certs.
up: certs ## Start gateway, mockupstream and redis; the gateway uses the mock over HTTPS
	$(COMPOSE_MOCK) up -d --build --force-recreate

up-real: ## Start gateway and redis; the gateway uses the real Anthropic API
	$(COMPOSE) up -d --build

down: ## Stop and remove all compose services
	$(COMPOSE_MOCK) --profile '*' down --remove-orphans

logs: ## Follow logs from all running services
	$(COMPOSE_MOCK) logs -f

bench: ## Load test mockupstream directly with k6 (after make up); BENCH_API, DURATION_S and STREAM pass through
	$(COMPOSE_MOCK) run --rm --no-deps -e TARGET_URL=$(MOCK_URL) k6

# Runs against gateway-bench, which always points at the mock and has no
# real upstream key, never the gateway from make up-real. Before k6 starts,
# the check waits up to about 15s for gateway-bench to log "listening", then
# cmd/benchguard reads its startup log and refuses to run unless it enabled
# at least one upstream and every "upstream enabled" line has a base_url of
# $(MOCK_URL) or under it, naming the upstream at fault otherwise.
#
# It checks the configuration the gateway resolved, not the variables it was
# given. A check of env vars has to know every upstream in advance, and an
# upstream it doesn't know about passes unseen: an override file that adds
# openai to SENTINEL_UPSTREAMS without OPENAI_BASE_URL silently gets the
# public default, api.openai.com. The log line is what the gateway will
# actually call, whatever the env says, and it covers custom upstreams too.
bench-gateway: certs ## Load test through gateway-bench, a gateway fixed to the mock; BENCH_API, BENCH_PREFIX, DURATION_S and STREAM pass through
	$(COMPOSE_BENCH) up -d --build --force-recreate --always-recreate-deps gateway-bench
	@id="$$($(COMPOSE_BENCH) ps -q gateway-bench)"; \
	if [ -z "$$id" ]; then echo "bench-gateway: gateway-bench is not running" >&2; exit 1; fi; \
	i=0; \
	while [ $$i -lt 30 ] && ! docker logs "$$id" 2>&1 | grep -q '"msg":"listening"'; do \
		[ "$$(docker inspect --format '{{.State.Running}}' "$$id")" = true ] || break; \
		sleep 0.5; i=$$((i + 1)); \
	done; \
	docker logs "$$id" 2>&1 | go run ./cmd/benchguard -mock $(MOCK_URL)
	$(COMPOSE_BENCH) run --rm --no-deps -e TARGET_URL=http://gateway-bench:8081 k6
