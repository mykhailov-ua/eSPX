# Stage 1: Modules caching
# Separate layer so go mod download is not re-run on source changes.
FROM golang:1.25.12-alpine AS modules
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

# Stage 2: Build
# CGO_ENABLED=0: produce statically linked binaries compatible with the distroless image.
# -tags timetzdata: embed IANA timezone database; required because distroless has no /usr/share/zoneinfo.
# -ldflags="-s -w": strip debug symbols and DWARF tables to minimise image size.
FROM golang:1.25.12-alpine AS builder
COPY --from=modules /go/pkg/mod /go/pkg/mod
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/tracker ./cmd/tracker
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/processor ./cmd/processor
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/auth ./cmd/auth
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/management ./cmd/management
# Separate binary: payment owns webhooks and settlement outbox outside management HTTP lifecycle.
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/payment ./cmd/payment
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/billing ./cmd/billing
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/notifier ./cmd/notifier
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/ivt-detector ./cmd/ivt-detector
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/fraud-scorer ./cmd/fraud-scorer
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/broker ./cmd/broker
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/log-shipper ./cmd/log-shipper
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -ldflags="-s -w" -o /bin/alertmanager-telegram ./cmd/alertmanager-telegram

# Stage 3: Final image
# distroless/static-debian12: no shell, no package manager, no libc; attack surface ~2 MB.
# USER nonroot:nonroot: containers run as UID 65532 to satisfy Kubernetes PodSecurityStandards.
FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/tracker /tracker
COPY --from=builder /bin/processor /processor
COPY --from=builder /bin/auth /auth
COPY --from=builder /bin/management /management
# Payment entrypoint when compose sets entrypoint: ["/payment"].
COPY --from=builder /bin/payment /payment
COPY --from=builder /bin/billing /billing
COPY --from=builder /bin/notifier /notifier
COPY --from=builder /bin/ivt-detector /ivt-detector
COPY --from=builder /bin/fraud-scorer /fraud-scorer
COPY --from=builder /bin/broker /broker
COPY --from=builder /bin/log-shipper /log-shipper
COPY --from=builder /bin/alertmanager-telegram /alertmanager-telegram
USER nonroot:nonroot
ENTRYPOINT ["/tracker"]
