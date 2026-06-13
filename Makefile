.PHONY: dev dev-down market-data tidy build fmt vet

dev: ## start local infra (NATS with JetStream)
	docker compose -f deploy/docker-compose.dev.yml up -d

dev-down: ## stop local infra
	docker compose -f deploy/docker-compose.dev.yml down

market-data: ## run the market-data service
	go run ./services/market-data

tidy: ## sync go.mod / go.sum
	go mod tidy

build: ## compile everything
	go build ./...

vet: ## static checks
	go vet ./...

fmt: ## format all Go files
	gofmt -w .
