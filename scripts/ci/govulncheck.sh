#!/usr/bin/env bash
# Scan module dependencies for known vulnerabilities. Local utility — not run in CI.
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

if ! command -v govulncheck >/dev/null 2>&1; then
	echo "Installing govulncheck..."
	go install golang.org/x/vuln/cmd/govulncheck@latest
fi

GOPATH="$(go env GOPATH)"
if [ -z "$GOPATH" ]; then
	GOPATH="$HOME/go"
fi

"$GOPATH/bin/govulncheck" ./...
