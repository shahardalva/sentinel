COMPOSE      := docker compose
COMPOSE_MOCK := $(COMPOSE) -f docker-compose.yml -f docker-compose.mock.yml --profile mock

.PHONY: fmt vet test certs run up up-real down logs bench bench-gateway

fmt: ## Format Go code with gofmt
	gofmt -w .

vet: ## Run go vet
	go vet ./...

test: ## Run tests with the race detector (needs cgo and a C compiler)
	go test -race ./...

certs: ## Write a dev CA and a server cert for mockupstream to ./certs
	go run ./cmd/devcerts -hosts localhost,127.0.0.1,mockupstream

run: ## Run the gateway on the host (needs the env vars from CLAUDE.md)
	go run ./cmd/gateway

certs/server.pem:
	$(MAKE) certs

up: certs/server.pem ## Start gateway, mockupstream and redis; the gateway uses the mock over HTTPS
	$(COMPOSE_MOCK) up -d --build

up-real: ## Start gateway and redis; the gateway uses the real Anthropic API
	$(COMPOSE) up -d --build

down: ## Stop and remove all compose services
	$(COMPOSE_MOCK) --profile '*' down --remove-orphans

logs: ## Follow logs from all running services
	$(COMPOSE_MOCK) logs -f

bench: ## Load test mockupstream directly with k6 (after make up); DURATION_S and STREAM pass through
	$(COMPOSE_MOCK) run --rm --no-deps k6

bench-gateway: ## Load test through the gateway with k6 (after make up), sending BENCH_API_KEY from .env
	TARGET_URL=http://gateway:8081 $(COMPOSE_MOCK) run --rm --no-deps k6
