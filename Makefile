GO       ?= go
VERSION  ?= $(shell (git describe --tags --always --dirty 2>/dev/null || echo dev) | sed 's/^v//')
REVISION ?= $(shell git rev-parse HEAD 2>/dev/null)
LDFLAGS  := -s -w \
	-X github.com/yuuki/xtsdb/internal/version.Version=$(VERSION) \
	-X github.com/yuuki/xtsdb/internal/version.Revision=$(REVISION)

# Backends started by `make deps` (see compose.yaml).
export XTSDB_TEST_REDIS_ADDRS     ?= 127.0.0.1:6379
export XTSDB_TEST_CASSANDRA_HOSTS ?= 127.0.0.1:9042
# The TiKV cluster started by `make deps-tikv`.
TIKV_PD ?= 127.0.0.1:2379

.PHONY: all
all: lint test build

.PHONY: build
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/xtsdb ./cmd/xtsdb

.PHONY: test
test: ## Unit tests (integration tests are skipped without backends)
	env -u XTSDB_TEST_REDIS_ADDRS -u XTSDB_TEST_CASSANDRA_HOSTS -u XTSDB_TEST_TIKV_PD $(GO) test -race ./...

.PHONY: test-integration
test-integration: deps ## All tests against Redis and Cassandra from compose.yaml
	$(GO) test -race -count=1 ./...

.PHONY: test-tikv
test-tikv: deps-tikv ## TiKV backend tests against Redis and TiKV from compose.yaml (Linux)
	docker compose up -d --wait redis
	env -u XTSDB_TEST_CASSANDRA_HOSTS XTSDB_TEST_TIKV_PD=$(TIKV_PD) \
		$(GO) test -race -count=1 ./internal/tikv/ ./internal/app/ ./cmd/xtsdb/

.PHONY: deps
deps:
	docker compose up -d --wait redis cassandra

.PHONY: deps-tikv
deps-tikv:
	docker compose --profile tikv up -d pd tikv
	@for i in $$(seq 120); do \
		curl -sf http://$(TIKV_PD)/pd/api/v1/stores | grep -q '"state_name": "Up"' && exit 0; sleep 1; \
	done; echo "TiKV did not start" >&2; exit 1

.PHONY: lint
lint:
	$(GO) vet ./...
	golangci-lint run

.PHONY: docker
docker:
	docker build --build-arg VERSION=$(VERSION) --build-arg REVISION=$(REVISION) -t xtsdb:$(VERSION) .

.PHONY: snapshot
snapshot: ## Build release archives into dist/ without publishing (needs GoReleaser v2)
	goreleaser release --snapshot --clean

.PHONY: release-notes
release-notes: ## Print the CHANGELOG.md section of VERSION, as the release workflow does
	@scripts/release-notes.sh $(VERSION)

.PHONY: up
up: ## Start the quickstart stack (xtsdb, Redis, Cassandra, Prometheus, Grafana)
	docker compose up -d --build

.PHONY: down
down:
	docker compose down -v

.PHONY: clean
clean:
	rm -rf bin dist
