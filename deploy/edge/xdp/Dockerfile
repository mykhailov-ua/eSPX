# XDP loader + Redis blacklist sync (host network, privileged).
# Build from repo root: docker build -f deploy/edge/xdp/Dockerfile .
FROM golang:1.25.12-bookworm AS modules
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

FROM golang:1.25.12-bookworm AS builder
COPY --from=modules /go/pkg/mod /go/pkg/mod
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/edge-xdp ./cmd/edge-xdp
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/edge-bpf-sync ./cmd/edge-bpf-sync

FROM debian:bookworm-slim
RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates iproute2 \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/edge-xdp /out/edge-bpf-sync /usr/local/bin/
COPY deploy/edge/xdp/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
