.PHONY: fmt gen lint test test-unit test-int test-chaos build proto

fmt:
	go fmt ./...

gen:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate

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

test-int: gen fmt
	go test -v ./tests/...

# Chaos tests kill real containers (testcontainers). Requires Docker. Skipped by -short elsewhere.
test-chaos: gen fmt
	go test -count=1 -v -run 'Chaos' -timeout 20m \
		./tests/... \
		./internal/auth/... \
		./internal/ads/... \
		./pkg/broker/server/... \
		./internal/management/... \
		2>&1 | tee /tmp/espx-chaos.log; \
	PROOFS=$$(grep -c 'chaos_proof fault=' /tmp/espx-chaos.log || true); \
	echo "chaos_proof lines: $$PROOFS"; \
	test "$$PROOFS" -ge 15

test: test-unit test-int

build: gen fmt
	docker build -t ad-event-processor:latest .

proto:
	go run github.com/bufbuild/buf/cmd/buf@latest generate
