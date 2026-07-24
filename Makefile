.PHONY: fmt gen lint test test-unit test-int test-alloc-gate test-full test-chaos test-broker-chaos-lab test-sentinel-chaos build proto check-local check-vuln

fmt:
	go fmt ./...

gen:
	bash scripts/codegen/gen.sh --proto

lint: gen fmt
	@if [ -z "$$(which golangci-lint 2> /dev/null)" ]; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.5; \
	fi
	@GOPATH=$$(go env GOPATH); \
	if [ -z "$$GOPATH" ]; then GOPATH=$$HOME/go; fi; \
	$$GOPATH/bin/golangci-lint run

test-unit: gen fmt
	go test -v -short ./internal/...

# Fast zero-alloc, fraud SLA, and RTB hot-path checks (no long benchmarks).
test-alloc-gate: gen fmt
	go test -short -count=1 -run 'ZeroAlloc|zeroAlloc_fraudScoring|FraudScoring_LatencySLA|ApplyRtbAuction_shadow_zeroAlloc|RecordRtbShadow|HTTP1Parse' ./internal/ingestion/...
	go test -run='^$$' -bench='Benchmark(HTTP1Parse$$|TrackRequest_ParseJSONOpt$$|Auction$$)' -benchmem -count=1 ./internal/ingestion/... ./internal/rtb/

test-int: gen fmt
	go test -v ./tests/...

# Chaos tests kill real containers (testcontainers). Requires Docker. Skipped by -short elsewhere.
test-chaos:
	bash scripts/chaos-drills/test_chaos.sh

# Broker durability lab chaos: slow fsync, page cache, CPU throttle, Redis outage, optional Sentinel stack.
test-broker-chaos-lab:
	bash scripts/chaos-drills/broker_chaos_lab.sh

# Sentinel failover chaos: docker compose redis+sentinel stack, stop master, verify go-redis client reads replica.
test-sentinel-chaos:
	bash scripts/chaos-drills/test_sentinel_failover.sh

test: test-unit test-int

# Full suite without -short; chaos tests run only in make test-chaos (CI chaos job).
test-full: fmt
	bash scripts/ci/full_test.sh

check-local:
	bash scripts/ci/local_check.sh

check-vuln:
	bash scripts/ci/govulncheck.sh

build: gen fmt
	docker build -t ad-event-processor:latest .

proto:
	bash scripts/codegen/gen.sh --proto
