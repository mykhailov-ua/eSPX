#!/usr/bin/env bash
# generate-dev-certs.sh: self-signed TLS cert for local edge :443 (M5-A). Not for production.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
openssl req -x509 -nodes -days 825 -newkey rsa:2048 \
	-keyout "$DIR/edge-dev.key" \
	-out "$DIR/edge-dev.crt" \
	-subj "/CN=edge.local/O=eSPX Dev/C=US" \
	-addext "subjectAltName=DNS:edge.local,DNS:localhost,IP:127.0.0.1"
chmod 600 "$DIR/edge-dev.key"
echo "Wrote $DIR/edge-dev.{crt,key}"
