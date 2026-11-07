GO       ?= go
VERSION  ?= $(shell (git describe --tags --always --dirty 2>/dev/null || echo dev) | sed 's/^v//')
REVISION ?= $(shell git rev-parse HEAD 2>/dev/null)
LDFLAGS  := -s -w \
	-X github.com/yuuki/xtsdb/internal/version.Version=$(VERSION) \
	-X github.com/yuuki/xtsdb/internal/version.Revision=$(REVISION)

# Tool versions. CI pins the same versions (.github/workflows); bump together.
GOLANGCI_LINT_VERSION ?= v2.15.0
GOVULNCHECK_VERSION   ?= v1.9.0
ACTIONLINT_VERSION    ?= v1.7.12
ZIZMOR_VERSION        ?= 1.32.0
FUZZTIME              ?= 30s
BENCH_COUNT           ?= 6

# `go run tool@version` would build a tool with the oldest Go its go.mod
# allows, and golangci-lint and govulncheck cannot load code that needs a
# later Go: build them with the toolchain of this module.
GO_TOOLCHAIN = $(shell $(GO) env GOVERSION)
GOLANGCI_LINT ?= GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
# uvx or pipx (preinstalled on GitHub-hosted runners) runs the pinned zizmor.
ZIZMOR ?= $(if $(shell command -v uvx 2>/dev/null),uvx zizmor@$(ZIZMOR_VERSION),pipx run zizmor==$(ZIZMOR_VERSION))

# Backends started by `make deps` (see compose.yaml).
export XTSDB_TEST_REDIS_ADDRS     ?= 127.0.0.1:6379
export XTSDB_TEST_CASSANDRA_HOSTS ?= 127.0.0.1:9042
# The TiKV cluster started by `make deps-tikv`.
TIKV_PD ?= 127.0.0.1:2379

# Runs a command without the backends, so that integration tests skip.
UNIT_ENV := env -u XTSDB_TEST_REDIS_ADDRS -u XTSDB_TEST_CASSANDRA_HOSTS -u XTSDB_TEST_TIKV_PD

.PHONY: all
all: lint test build

.PHONY: check
check: lint lint-actions test cross ## Local pre-PR gate: what the required CI checks run without backends

.PHONY: build
build: ## bin/xtsdb
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/xtsdb ./cmd/xtsdb

.PHONY: cross
cross: ## Build the release targets, and vet the build without the TiKV backend
	@set -e; for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do \
		echo "== $$target"; \
		GOOS=$${target%/*} GOARCH=$${target#*/} CGO_ENABLED=0 $(GO) build -o /dev/null ./cmd/xtsdb; \
	done
	$(GO) vet -tags notikv ./...

.PHONY: test
test: ## Unit tests with the race detector (integration tests are skipped without backends)
	$(UNIT_ENV) $(GO) test -race ./...

.PHONY: test-race
test-race: test ## Alias of test, which already runs with the race detector

.PHONY: test-fuzz
test-fuzz: ## Run every fuzz target for FUZZTIME
	@set -e; for pkg in $$($(GO) list ./...); do \
		for f in $$($(GO) test -list '^Fuzz' $$pkg | grep '^Fuzz' || true); do \
			echo "== $$pkg $$f"; \
			$(UNIT_ENV) $(GO) test -run='^$$' -fuzz="^$$f\$$" -fuzztime=$(FUZZTIME) $$pkg; \
		done; \
	done

.PHONY: test-integration
test-integration: deps ## All tests against Redis and Cassandra from compose.yaml
	$(GO) test -race -count=1 ./...

.PHONY: test-tikv
test-tikv: deps-tikv ## TiKV backend tests against Redis and TiKV from compose.yaml (Linux)
	docker compose up -d --wait redis
	env -u XTSDB_TEST_CASSANDRA_HOSTS XTSDB_TEST_TIKV_PD=$(TIKV_PD) \
		$(GO) test -race -count=1 ./internal/tikv/ ./internal/app/ ./cmd/xtsdb/

.PHONY: bench
bench: ## Benchmarks without backends into bench.txt (compare runs with benchstat)
	$(UNIT_ENV) $(GO) test -run='^$$' -bench=. -benchmem -count=$(BENCH_COUNT) ./... | tee bench.txt

.PHONY: deps
deps: ## Start Redis and Cassandra with Docker Compose
	docker compose up -d --wait redis cassandra

.PHONY: deps-tikv
deps-tikv: ## Start a single-node TiKV with Docker Compose (Linux)
	docker compose --profile tikv up -d pd tikv
	@for i in $$(seq 120); do \
		curl -sf http://$(TIKV_PD)/pd/api/v1/stores | grep -q '"state_name": "Up"' && exit 0; sleep 1; \
	done; echo "TiKV did not start" >&2; exit 1

.PHONY: lint
lint: ## go vet, golangci-lint (linters and formatters) and go.mod tidiness
	$(GO) vet ./...
	$(GOLANGCI_LINT) run
	$(GO) mod tidy -diff

.PHONY: fmt
fmt: ## Format the code (gofmt, goimports)
	$(GOLANGCI_LINT) fmt

.PHONY: lint-actions
lint-actions: ## Lint (actionlint) and audit (zizmor) the GitHub Actions workflows
	GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	$(ZIZMOR) $(if $(GH_TOKEN),,--offline) .

.PHONY: vuln
vuln: ## Known vulnerabilities reachable from the code
	GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

.PHONY: docker
docker: ## Container image
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
down: ## Stop the compose stack and delete its volumes
	docker compose down -v

.PHONY: clean
clean:
	rm -rf bin dist bench.txt
