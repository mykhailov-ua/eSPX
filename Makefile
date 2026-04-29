.PHONY: fmt lint test test-unit test-int build

fmt:
	go fmt ./...

lint: fmt
	@if [ -z "$$(which golangci-lint 2> /dev/null)" ]; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi
	$$(go env GOPATH)/bin/golangci-lint run

test-unit: fmt
	go test -v ./tests/unit/...

test-int: fmt
	go test -v ./tests/integration/...

test: test-unit test-int

build: fmt
	docker build -t ad-event-processor:latest .
