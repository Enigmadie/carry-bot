# Load .env (gitignored) so secrets like BYBIT_API_KEY reach the services.
# The leading `-` ignores a missing file; `export` pushes the values into the
# environment of every recipe, where os.Getenv picks them up.
-include .env
export

.PHONY: dev dev-down market-data strategy order portfolio tidy build fmt vet

dev: ## start local infra (NATS with JetStream)
	docker compose -f deploy/docker-compose.dev.yml up -d

dev-down: ## stop local infra
	docker compose -f deploy/docker-compose.dev.yml down

market-data: ## run the market-data service
	go run ./services/market-data

strategy: ## run the strategy service
	go run ./services/strategy

order: ## run the order service (EXCHANGE=mock default; bybit needs API keys)
	go run ./services/order

portfolio: ## run the portfolio service (needs Postgres via DATABASE_URL)
	go run ./services/portfolio

tidy: ## sync go.mod / go.sum
	go mod tidy

build: ## compile everything
	go build ./...

vet: ## static checks
	go vet ./...

fmt: ## format all Go files
	gofmt -w .
