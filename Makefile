GO       ?= go
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REVISION ?= $(shell git rev-parse HEAD 2>/dev/null)
LDFLAGS  := -s -w \
	-X github.com/yuuki/xtsdb/internal/version.Version=$(VERSION) \
	-X github.com/yuuki/xtsdb/internal/version.Revision=$(REVISION)

# Backends started by `make deps` (see compose.yaml).
export XTSDB_TEST_REDIS_ADDRS     ?= 127.0.0.1:6379
export XTSDB_TEST_CASSANDRA_HOSTS ?= 127.0.0.1:9042

.PHONY: all
all: lint test build

.PHONY: build
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/xtsdb ./cmd/xtsdb

.PHONY: test
test: ## Unit tests (integration tests are skipped without backends)
	env -u XTSDB_TEST_REDIS_ADDRS -u XTSDB_TEST_CASSANDRA_HOSTS $(GO) test -race ./...

.PHONY: test-integration
test-integration: deps ## All tests against Redis and Cassandra from compose.yaml
	$(GO) test -race -count=1 ./...

.PHONY: deps
deps:
	docker compose up -d --wait redis cassandra

.PHONY: lint
lint:
	$(GO) vet ./...
	golangci-lint run

.PHONY: docker
docker:
	docker build --build-arg VERSION=$(VERSION) --build-arg REVISION=$(REVISION) -t xtsdb:$(VERSION) .

.PHONY: up
up: ## Start the quickstart stack (xtsdb, Redis, Cassandra, Prometheus, Grafana)
	docker compose up -d --build

.PHONY: down
down:
	docker compose down -v

.PHONY: clean
clean:
	rm -rf bin dist
