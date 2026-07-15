# Development Guide

Setup, tooling, and operational procedures for the eSPX codebase.

## Documentation index

| Document | Scope |
| :--- | :--- |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Topology, patterns (appendix A), microservices (appendix B) |
| [EDGE.md](./EDGE.md) | Ingress, Redis, UDP control (Part III) |
| [DATABASE.md](./DATABASE.md) | PostgreSQL + ClickHouse |
| [GO.md](./GO.md) | gnet, zero-alloc, filter engine |

---

## Requirements

- Go 1.25+
- Docker and Docker Compose
- `buf` CLI (or `make proto` which invokes buf via `go run`)
- `lefthook` (optional, for git hooks)

---

## Quick Start

```bash
cp .env.example .env
# Optional: deploy/geoip/GeoLite2-Country.mmdb for production geo
bash scripts/local-dev/dev_stack.sh build
bash scripts/local-dev/dev_stack.sh full
bash scripts/local-dev/dev_preflight.sh
```

Full stack adds `tracker-1..3`, `nginx`, `prometheus`, `grafana`, `alertmanager`, sentinels, replicas.

`dev_stack.sh` profiles:

| Command | Services started |
| :--- | :--- |
| `bash scripts/local-dev/dev_stack.sh infra` | db, redis-0…5, clickhouse |
| `bash scripts/local-dev/dev_stack.sh full` | infra + processor, tracker-0, auth, management, payment, billing |
| `bash scripts/local-dev/dev_stack.sh sentinel` | redis-0, replica, sentinel-0…2 |
| `bash scripts/local-dev/dev_stack.sh status` | `docker compose ps` |
| `bash scripts/local-dev/dev_stack.sh down` | tear down compose stack |

Pre-deploy topology check:

```bash
sh scripts/redis-ops/verify_redis_topology.sh .env
```

---

## K3s cold path (local)

Cold-path services (auth, management, payment, billing, notifier, processor, ivt-detector) run in k3s namespace `espx`. Hot path (tracker, nginx) uses hostNetwork in `espx-edge` (see [K3s hot path](#k3s-hot-path)) or compose host network on edge nodes.

**Prereqs:** k3s installed (`bash scripts/k8s/install_k3s.sh`), Terraform in `PATH` (`~/bin/terraform` or package), Docker for compose data plane.

```bash
cp .env.example .env   # if missing
bash scripts/k8s/k8s_cold_path_up.sh
```

The script starts compose **infra** (Postgres, Redis x4, ClickHouse), applies DB migrations (`cmd/migrate-cold-path`), syncs GeoIP to `/var/lib/espx/geoip`, builds/imports the image, and runs `terraform apply` in `deploy/terraform/envs/local/`.

| NodePort | Service | Purpose |
| :--- | :--- | :--- |
| 30188 | management | Admin HTTP `/health`, `/metrics` |
| 30187 | payment webhook | Stripe webhooks |
| 30186 | processor | `/health`, `/metrics` |

Verify:

```bash
export KUBECONFIG=~/.kube/config-espx
bash scripts/k8s/k8s_cold_path_smoke.sh
curl -s "http://$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type==\"InternalIP\")].address}'):30188/health"
```

Prometheus scrape for k3s cold path (host-side Prometheus):

```bash
bash scripts/k8s/render_prometheus_k3s.sh
# use deploy/monitoring/prometheus-k3s.rendered.yaml
```

Terraform outputs: `cd deploy/terraform/envs/local && terraform output`.

---

## K3s staging

Staging cold path targets a **remote k3s** cluster with an **external data plane** (managed Postgres, 4× Redis, ClickHouse). No compose `host_ip` dependency.

```bash
cp deploy/terraform/envs/staging/terraform.tfvars.example deploy/terraform/envs/staging/terraform.tfvars
# fill TF_VAR_* / tfvars secrets (DB_DSN, tokens, registry image)
bash scripts/k8s/k8s_staging_apply.sh
```

| Item | Local | Staging |
| :--- | :--- | :--- |
| k3s bootstrap | `install_k3s.sh` via local terraform | Pre-provisioned cluster kubeconfig |
| Image | `k8s_import_image.sh` → `ad-event-processor:latest` | Registry (`ghcr.io/...`), `imagePullPolicy: Always` |
| ConfigMap/Secret | `*.yaml.tpl` with node `host_ip` | `*.staging.tpl` with `redis_addrs`, DSN vars |
| `ENV` / filter | `development`, 5000 ms | `production`, 100 ms |
| Control flags | off (local dev) | `MIGRATION_FENCE_ENABLED`, `UDP_CONTROL_ENABLED`, `QUOTA_AUTO_REPAIR`, `QUOTA_MODE=live` in `configmap-env.staging.tpl` |
| Secrets in git | Dev placeholders | `terraform.tfvars` (gitignored) or CI `TF_VAR_*` |

Optional registry auth creates `espx-registry` pull secret when `registry_server` + `registry_password` are set.

By default staging skips NodePort Services and applies `ingress-cold-path.yaml.tpl` instead (`exclude_nodeports`, `enable_ingress`).

Manifest layout:

```
deploy/k8s/
  base/           # namespace, configmap/secret *.tpl (local + staging)
  apps/           # cold-path Deployments/Services/NodePorts, ingress tpl
  hot-path/       # tracker x4 + nginx DaemonSet (hostNetwork)
  overlays/staging/  # kustomize reference for registry image patches
deploy/terraform/envs/
  local/          # k3s bootstrap + cold path (+ optional hot_path.tf)
  staging/        # remote k3s + external data plane + Ingress
```

---

## K3s hot path

Ingestion (`tracker` ×4 + OpenResty `:8180`) runs in namespace `espx-edge` with **hostNetwork** — same SLA rationale as compose host mode.

**Prereqs:** cold path up (`k8s_cold_path_up.sh`), compose **infra** only (not full stack nginx/trackers), management NodePort `30188`.

**Port conflict:** compose `nginx` and k8s `nginx-edge` both bind host `:8180` — stop compose hot path before apply (`docker compose stop nginx tracker-0 tracker-1 tracker-2 tracker-3`).

```bash
# optional edge tuning on the node (sysctl/NIC)
bash scripts/edge-tuning/edge_sysctl.sh install
bash scripts/k8s/k8s_hot_path_up.sh
bash scripts/k8s/k8s_hot_path_smoke.sh
```

| Port | Component |
| :--- | :--- |
| 8180 | OpenResty edge (`/track`, `/admin` proxy) |
| 8181–8184 | gnet trackers |

Host paths synced by the script: `/var/lib/espx/geoip`, `/var/lib/espx/logs`. OpenResty config/lua ship as in-cluster ConfigMaps (`nginx-edge-conf`, `nginx-edge-lua`).

hostNetwork trackers reach compose Postgres/Redis via **loopback** (`127.0.0.1`), not node InternalIP.

---

## Code Generation

| Target | Command | Output |
| :--- | :--- | :--- |
| `make proto` | `scripts/codegen/gen.sh --proto` | `internal/*/pb/*` (vtproto) |
| `make gen` | `scripts/codegen/gen.sh` (sqlc) | `internal/*/db/*` |
| `task gen` | `scripts/codegen/gen.sh --all` | sqlc + templ (if installed) + buf |

Protobuf sources live in `api/` (flat layout). sqlc pinned to **v1.28.0** (Go 1.25-compatible).

---

## Scripts (`scripts/`)

One subdirectory level: `bash scripts/<area>/<name>.sh`. File names use **snake_case** with domain prefix (R2); shared paths in `lib/paths.sh`.

### Local dev and compose

| Script | Purpose |
| :--- | :--- |
| `dev_stack.sh` | Compose lifecycle: `infra`, `full`, `sentinel`, `down`, `status`, `build` |
| `dev_preflight.sh` | `check_deps.sh` then `smoke_local.sh` |
| `check_deps.sh` | Preflight: Postgres, six Redis shards, ClickHouse ports/migrations |
| `local_check.sh` | Lint, alloc gate, unit+integration tests, docker build (local pre-push) |
| `govulncheck.sh` | Dependency vulnerability scan (`make check-vuln`) |
| `full_test.sh` | `go test ./... -skip Chaos` (CI + `make test-full`) |
| `smoke_local.sh` | Tracker/processor `/health`, edge `/metrics/edge`, 4× Redis PING/AOF; SKIP when stack down |
| `k8s_cold_path_up.sh` | Compose infra + image import + terraform apply for k3s cold path |
| `k8s_cold_path_smoke.sh` | k3s cold-path pods Ready + management/processor NodePort health |
| `k8s_staging_apply.sh` | Terraform apply for staging env (remote k3s + external data plane) |
| `k8s_hot_path_up.sh` | hostNetwork trackers + OpenResty in `espx-edge` namespace |
| `k8s_hot_path_smoke.sh` | Tracker :8181-8184 `/health` + nginx :8180 listen check |
| `k8s_import_image.sh` | Build image and import into k3s containerd |
| `install_k3s.sh` | Single-node k3s bootstrap (Traefik disabled) |
| `render_prometheus_k3s.sh` | Render `prometheus-k3s.yaml` with node InternalIP |
| `gen.sh` | Codegen: default sqlc; flags `--proto`, `--templ`, `--all` |

### Performance and CI

| Script | Purpose |
| :--- | :--- |
| `perf_gate_run.sh` | Perf gate: worktree baseline + `perf_gate_bench.sh` + `perf_gate.go` |
| `perf_gate_bench.sh` | Hot-path benchmarks for PR gate (`internal/ingestion`) |
| `perf_gate.go` | Zero-alloc check + benchstat; `--cpu-only` for nightly |
| `perf_baseline_gate.sh` | Nightly benchstat vs cached baseline (seeds on miss) |
| `run_bench.sh` | Shared `go test -bench` runner (`<regex> <pkg...>`) |
| `nightly_bench_job.sh` | Nightly: `redis` or `broker` bench + gate + baseline update |
| `escape_nightly_job.sh` | Escape analysis; second arg enables regression gate |
| `stabilize_cpu.sh` | CPU performance governor (perf CI) |
| `edge_nic_tune.sh` | Ingress NIC RX ring max + IRQ/RSS spread (`deploy/edge/`) |
| `edge_sysctl.sh` | Ingress sysctl install/verify (`deploy/edge/99-espx-edge.conf`) |
| `edge_baseline.sh` | Minimal Prometheus SLA snapshot for edge baseline |
| `install_benchstat.sh` | Ensures `benchstat` on PATH |

### Chaos and failover

| Script | Purpose |
| :--- | :--- |
| `test_chaos.sh` | testcontainers chaos suite; requires ≥46 `chaos_proof` lines (`CHAOS_MIN_PROOFS`) |
| `test_sentinel_failover.sh` | Sentinel promote/failover against compose stack |
| `sentinel_chaos_env.sh` | CI: copy `.env.example` with sentinel test password |

### Redis operations

| Script | Purpose |
| :--- | :--- |
| `verify_redis_topology.sh` | `REDIS_ADDRS` count vs `REDIS_SHARD_COUNT` (default 4) |
| `redis_reconcile_post_deploy.sh` | Read-only drift check: `config:*`, `blacklist:manual` on all shards |
| `redis_migrate_campaign.sh` | Move campaign keys between shards (StaticSlot) |
| `campaign_shard.go` | `go run ./scripts/redis-ops/campaign_shard.go <uuid> [N]` — shard index |

### Production

| Script | Purpose |
| :--- | :--- |
| `log_evacuate.sh` | S3 upload of `.log.zst.ready` segments (`Dockerfile.log-evacuator`) |

Workflow wiring: `.github/workflows/` (`ci.yaml`, `perf-gate.yaml`, `perf-nightly.yaml`, `sentinel-chaos.yaml`).

---

## Make Targets

| Target | Purpose |
| :--- | :--- |
| `make fmt` | `go fmt ./...` |
| `make gen` | `scripts/codegen/gen.sh` (sqlc v1.28.0) |
| `make proto` | `scripts/codegen/gen.sh --proto` (buf → vtproto) |
| `make lint` | gen + fmt + golangci-lint |
| `make test-unit` | `go test -short ./internal/...` |
| `make test-int` | `go test ./tests/...` |
| `make test-alloc-gate` | zero-alloc + fraud SLA in `./internal/ingestion/`; `BenchmarkAuction` 0 allocs in `./internal/rtb/` |
| `make test-chaos` | `scripts/chaos-drills/test_chaos.sh` (Docker, ≥46 `chaos_proof` lines) |
| `make test-sentinel-chaos` | `scripts/chaos-drills/test_sentinel_failover.sh` |
| `make test` | test-unit + test-int |
| `make test-full` | `scripts/ci/full_test.sh` (chaos: `make test-chaos`) |
| `make check-local` | `scripts/ci/local_check.sh` — lint, alloc gate, test, build |
| `make check-vuln` | `scripts/ci/govulncheck.sh` |
| `make build` | `docker build -t ad-event-processor:latest .` |

---

## Taskfile (optional)

Requires [Task](https://taskfile.dev). Overlaps with `make` where noted.

| Task | Purpose |
| :--- | :--- |
| `task gen` | `scripts/codegen/gen.sh --all` (sqlc + templ if installed + buf) |
| `task docker-up` | `scripts/local-dev/dev_stack.sh infra` |
| `task docker-down` | `scripts/local-dev/dev_stack.sh down` |
| `task check-deps` | `scripts/ci/check_deps.sh` |
| `task dev-preflight` | `scripts/local-dev/dev_preflight.sh` |
| `task perf-gate` | `scripts/perf-gate/perf_gate_run.sh` vs `main` (worktree `../baseline-local`) |
| `task test-full` | `go test -race ./...` (not the same as `make test-full`) |

---

## Git Hooks (Lefthook)

```bash
lefthook install
```

- **pre-commit:** `make lint`
- **pre-push:** `make test`

---

## Ports and Services

| Service | Port | Binary |
| :--- | :--- | :--- |
| Nginx | 8180 | — |
| Tracker | 8181–8184 | `cmd/tracker` |
| Payment HTTP (webhooks, HTMX demo) | 8187 | `cmd/payment` |
| Processor | 8186 | `cmd/processor` |
| Management REST | 8188 | `cmd/management` |
| Auth gRPC | 51051 | `cmd/auth` |
| Auth metrics | 9091 | `cmd/auth` |
| Payment gRPC | 51052 | `cmd/payment` |
| Settlement gRPC | 51053 | `cmd/management` (sidecar) |
| Billing gRPC | 51054 | `cmd/billing` |
| Notifier gRPC | 8085 | `cmd/management` (when notifier channels configured) |
| Tracker metrics | 9090 (sidecar); `/metrics` also on :8181–8184 (gnet) | `cmd/tracker` |
| Redis shards | 6479–6482 | `redis-0` … `redis-3` |
| Redis Sentinel | 26379–26381 | `sentinel-0` … `sentinel-2` |
| PostgreSQL | 5430 | `db` |
| ClickHouse native / HTTP | 9000 / 8123 | `clickhouse` |
| Prometheus | 9190 | — |
| Alertmanager | 9093 | — |
| Telegram proxy | 8222 | `cmd/alertmanager-telegram` |
| Grafana | 3100 | — |

Host networking (`NET_MODE=host`) is default for app services. Stateful stores publish ports from the `database` bridge network.

### Not in compose

| Binary | Purpose |
| :--- | :--- |
| `cmd/broker` | mmap log broker |
| `cmd/log-shipper` | Tails tracker logs to broker |
| `cmd/dlq` | DLQ archive / requeue / restore |
| `cmd/admin` | Cobra dev CLI (users, seed, budget reset) |

`billing` is in the default `dev_stack.sh full` profile but optional for minimal ingest-only stacks. Notifier gRPC starts inside management when channel credentials are set.

Broker HA lab: `deploy/broker/` (optional). `docker compose -f deploy/broker/docker-compose.yaml up -d`. HAProxy exposes `:9092` (leader-only produce via `/leaderz`) and `:9093` (any healthy node for fetch). Sentinel overlay and chaos drills: see `deploy/broker/README.md` and `scripts/chaos-drills/broker_chaos_lab.sh`. Override binary: `ESPX_BROKER_BIN=/path/to/espx-broker`.

RTB live soak (optional): `docker compose -f docker-compose.yaml -f deploy/rtb/docker-compose.override.yaml up -d tracker-0 … tracker-3`. Default `.env` keeps `RTB_MODE=off`. See `deploy/rtb/README.md`.

---

## Environment Variables (selected)

Full template: `.env.example`. Required at startup: `SERVER_PORT`, `DB_DSN`, `REDIS_ADDRS`, `TOKEN_SYMMETRIC_KEY`.

### Redis

```bash
REDIS_ADDRS=127.0.0.1:6479,...,127.0.0.1:6482   # production: exactly 4
# Optional Sentinel for Go services:
# REDIS_SENTINEL_ADDRS=127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381
# REDIS_MASTER_NAMES=espx-shard-0,...,espx-shard-3
REDIS_BREAKER_FAIL_THRESHOLD=150
REDIS_BREAKER_OPEN_TIMEOUT_MS=5000
```

### Payment

```bash
PAYMENT_SERVER_PORT=51052
PAYMENT_WEBHOOK_PORT=8187
SETTLEMENT_SERVER_PORT=51053
PAYMENT_DB_DSN=postgres://...@127.0.0.1:5431/espx_payment?sslmode=disable  # separate db-payment container
PAYMENT_INTERNAL_TOKEN=...      # management to payment gRPC
SETTLEMENT_INTERNAL_TOKEN=...   # payment outbox to settlement gRPC
STRIPE_SECRET_KEY=              # unset = MockProvider; set = StripeProvider stub (checkout still mock)
STRIPE_WEBHOOK_SECRET=          # required for live webhook signature verification
```

#### Stripe checkout policy

Checkout is mock-only in the current tree. `createStripeCheckoutSession` (`internal/payment/provider_stripe.go`) returns `ErrProviderNotConfigured` even when `STRIPE_SECRET_KEY` is set.

| Mode | `STRIPE_SECRET_KEY` | Checkout behavior |
|------|---------------------|-------------------|
| Mock (default) | unset | `MockProvider` — deterministic `pi_mock_*` refs and `checkout.stripe.dev` URLs |
| Stripe stub | set | `StripeProvider` selected; checkout API still returns `ErrProviderNotConfigured` |

`NewProvider` (`internal/payment/provider.go`) selects mock vs Stripe at startup. `NewStripeProvider` delegates checkout to `createStripeCheckoutSession`. Boot logs `checkout_api=pending_stripe_go` when a secret key is present.

Local dev: leave `STRIPE_SECRET_KEY` empty and use `MockProvider` for settlement, refund, and recon chaos tests. Webhook handlers accept mock `provider_ref` values without Stripe network calls.

Live checkout is not implemented: requires `stripe-go` in `createStripeCheckoutSession`, live webhooks in `internal/payment/http_webhook.go`, and HTMX checkout URL wiring in `internal/payment/http_htmx.go`. No PAN storage in local databases.

Payment schema is auto-applied on `cmd/payment` startup (embedded goose migrations). With compose, `db-payment` on `PAYMENT_DB_PORT` (default 5431) holds only the `payment` schema. Omit `PAYMENT_DB_DSN` to fall back to `DB_DSN` (single-DB dev).

### Billing

```bash
BILLING_SERVER_PORT=51054
BILLING_SERVER_HOST=127.0.0.1
BILLING_INTERNAL_TOKEN=...   # management to billing gRPC (x-internal-token)
BILLING_INVOICE_WORKER_ENABLED=true
BILLING_INVOICE_NOTIFY_RECIPIENT=...   # optional; falls back to TELEGRAM_CHAT_ID
```

#### Internal token rotation

Per-service gRPC callers authenticate with metadata `x-internal-token`. Rotate in this order to avoid downtime:

1. Generate a new secret per service (`openssl rand -hex 32`).
2. Deploy **servers** with both old and new tokens accepted (dual-verify window) or brief maintenance window.
3. Update **clients** (`payment` → `SETTLEMENT_INTERNAL_TOKEN`, `management` → `BILLING_INTERNAL_TOKEN`, `ivt-detector` → `SETTLEMENT_INTERNAL_TOKEN` for `BlockIP`).
4. Remove the old token from server config after all clients report healthy.
5. Record rotation date in your ops runbook.

| Token env | Server | Clients |
|-----------|--------|---------|
| `SETTLEMENT_INTERNAL_TOKEN` | management settlement gRPC | payment outbox, ivt-detector |
| `PAYMENT_INTERNAL_TOKEN` | payment gRPC | management |
| `BILLING_INTERNAL_TOKEN` | billing gRPC | management |

Apply schema: `internal/billing/migrations/00001_init_billing_schema.sql` (goose Up section).

HTMX endpoints (require `BILLING_INTERNAL_TOKEN` on management):

- `GET /admin/customers/{id}/billing`
- `POST /admin/customers/{id}/billing/invoices` (`billing_month=YYYY-MM`)

### Notifier

```bash
NOTIFIER_PORT=8085
NOTIFIER_WORKER_INTERVAL_MS=1000
NOTIFIER_WORKER_BATCH_SIZE=10
NOTIFIER_BREAKER_FAIL_THRESHOLD=3
NOTIFIER_BREAKER_SUCCESS_THRESHOLD=2
NOTIFIER_BREAKER_OPEN_TIMEOUT_MS=30000
# At least one provider credential:
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
SLACK_WEBHOOK_URL=
SMTP_HOST=
SMTP_PORT=
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_SENDER=
SMS_PROVIDER_URL=
SMS_API_TOKEN=
SMS_DEFAULT_RECIPIENT=
```

Apply schema: `internal/notifier/migrations/00001_init_notifier_schema.sql` (goose Up section).

### Lifecycle

```bash
SHUTDOWN_TIMEOUT_MS=15000   # SIGTERM drain budget (all services)
DRAIN_TIMEOUT_MS=10000      # tracker connection drain
WAIT_TIMEOUT_MS=5000        # gnet shutdown wait
```

### Filtering

```bash
TTC_MIN_MS=300
TTC_FAIL_CLOSED=false           # set true in prod after bypass rate review
RATE_LIMIT_PER_MIN=100
DUPLICATE_TTL_SEC=45
FILTER_TIMEOUT_MS=5000
CLICK_AMOUNT=0.1                # dollars to micro-units internally
IMPRESSION_AMOUNT=0.01
```

### Fraud scoring (cold path)

ML scoring and enforcement run on the cold path (`ivt-detector` / `fraud-scorer` → management outbox → Redis). The hot path reads `ml:score:boost:{campaign_id}` via `SettingsWatcher`; it never imports `internal/fraudscoring`. Architecture: [ARCHITECTURE.md](./ARCHITECTURE.md#fraud-scoring-cold-path).

**Default:** `FRAUD_SCORING_ENABLED=false` (production stays off until explicit ops enable).

#### Environment variables

| Variable | Default | Role |
| :--- | :--- | :--- |
| `FRAUD_SCORING_ENABLED` | `false` | Kill-switch for scorer and enforcement |
| `FRAUD_SCORER_STANDALONE` | `false` | When `true`, `ivt-detector` skips embedded scorer |
| `FRAUD_SCORING_SCAN_INTERVAL_MS` | `300000` | Batch scan cadence (5 min) |
| `FRAUD_SCORING_BATCH_SIZE` | `1000` | Rows per inference batch |
| `FRAUD_SCORING_MODEL_PATH` | — | Path to LightGBM `model.txt` artifact |
| `ML_SYNC_INTERVAL_MS` | `60000` | Model epoch publish cadence |
| `ML_OUTBOX_PENDING_LIMIT` | `500` | Pause enforcement when outbox backlog exceeds limit |
| `ML_SYNC_SHARD_TIMEOUT_SEC` | `180` | Auto-rollback shard stuck in `SYNC` |
| `ML_SCORE_BOOST_TTL_SEC` | `3600` | Redis boost key TTL |
| `SETTLEMENT_INTERNAL_TOKEN` | — | gRPC auth for ivt-detector → management enqueue |

Stale ML channel threshold: `2 × ML_SYNC_INTERVAL_MS` (triggers suspect-tier tighten).

#### Staging enable

```bash
FRAUD_SCORING_ENABLED=true
FRAUD_SCORING_SCAN_INTERVAL_MS=300000
FRAUD_SCORING_BATCH_SIZE=1000
FRAUD_SCORING_MODEL_PATH=/path/to/model.txt
SETTLEMENT_INTERNAL_TOKEN=<same as management>
```

After 24h shadow scoring, run `scripts/fraud-scoring/shadow_precision_report.sql` in ClickHouse before enabling boost enforcement in production.

#### Standalone worker

```bash
docker compose --profile fraud-scorer up -d fraud-scorer
```

```bash
FRAUD_SCORING_ENABLED=true
FRAUD_SCORER_STANDALONE=true
FRAUD_SCORING_MODEL_PATH=/var/fraudscore/artifacts/model.txt
FRAUD_SCORING_SCAN_INTERVAL_MS=300000
```

k8s: `deploy/k8s/apps/deployment-fraud-scorer.yaml` (Deployment + weekly retrain CronJob).

#### Training pipeline

```bash
bash scripts/fraud-scoring/train.sh
```

Flow: ClickHouse export (Parquet, 7–30d) → Python train (LightGBM + Isolation Forest) → validate holdout → export `model.txt` + `metadata.json` → `fraud-scorer` registry watcher detects new artifact → `FraudModelSyncOrchestrator` rolling sync.

Processor micro-batch (`FRAUD_SCORING_ENABLED` on `cmd/processor`) writes `ml:score:boost:{campaign_id}` directly when `ad_processor_stream_lag_seconds` &lt; 30.

#### Model rollback

If a deployed model shows high false positives:

```sql
-- Retire bad version, re-activate previous
UPDATE ml_model_versions SET status = 'RETIRED' WHERE id = <bad_id>;
UPDATE ml_model_versions SET status = 'ACTIVE' WHERE id = <prev_id>;
```

Then trigger sync via management or restart `fraud-scorer` — outbox pushes previous `ml:model:hash` to all shards. Or use emergency kill-switch below.

### Fraud scoring operator runbook

#### 1. Emergency Kill-Switch
If ML scoring or enforcement causes unexpected behavior or high false-positive rates:
- Set `FRAUD_SCORING_ENABLED=false` on `ivt-detector` and/or `fraud-scorer`.
- Restart the affected worker to halt new scoring and threat enqueuing.
- k8s: `kubectl set env deployment/fraud-scorer FRAUD_SCORING_ENABLED=false`

#### 2. Manual Overrides (False Positive Mitigation)
If a legitimate user's IP is blacklisted or a campaign's score is incorrectly boosted, operators can apply manual overrides via the management API.

##### Clear Campaign Score Boost
To clear an active score boost for a campaign:
```bash
curl -X POST http://localhost:8080/admin/fraud-scoring/overrides \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"campaign_id": "YOUR-CAMPAIGN-UUID"}'
```
*Effect:* Instantly creates an `ML_SCORE_BOOST` outbox event with `Boost: 0` and `TTL: 0`, which deletes the Redis key across all shards and publishes a cache invalidation.

##### Remove IP from Blacklist
To unblock an IP that was falsely blacklisted by ML:
```bash
curl -X POST http://localhost:8080/admin/fraud-scoring/overrides \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"ip": "1.2.3.4"}'
```
*Effect:* Deletes the IP from the `ip_blacklist` table in Postgres, creates an `UPDATE_BLACKLIST` outbox event with `Action: "remove"`, and publishes a quarantine removal message to Redis.

##### Combined Override
Both overrides can be issued in a single request:
```bash
curl -X POST http://localhost:8080/admin/fraud-scoring/overrides \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"campaign_id": "YOUR-CAMPAIGN-UUID", "ip": "1.2.3.4"}'
```

#### 3. Audit logs

Manual overrides are logged to `audit_logs` with action types `FRAUD_CLEAR_BOOST` and `FRAUD_REMOVE_FALSE_POSITIVE`.

#### 4. Fraud scoring chaos tests

ML-specific chaos proofs extend the global gate (`CHAOS_MIN_PROOFS=46` in `scripts/chaos-drills/test_chaos.sh`). Key scenarios in `internal/ivtdetector/` and `internal/ingestion/`:

`fraud_outbox_backpressure`, `fraud_exactly_once`, `fraud_management_retry`, `fraud_epoch_gap_tighten`, `fraud_epoch_gap_loosen_block`, `fraud_worker_down`, `fraud_model_cutover_rollback`, `fraud_budget_invariant`.

Every enforcement test that touches spend must pass `AssertBudgetInvariant` (±1 currency micro-unit) and `current_spend ≤ budget_limit` in Postgres.

---

## Admin CLI (`cmd/admin`)

```bash
go run cmd/admin/main.go user create --email=... --password=...
go run cmd/admin/main.go db seed          # 100 customers, 1000 campaigns
go run cmd/admin/main.go budget reset --campaign-id=...
```

---

## DLQ Utility (`cmd/dlq`)

```bash
# Archive DLQ to disk
go run cmd/dlq/main.go -action=archive -stream=ad:events:dlq -dest=dlq_archive.bin -batch=1000

# Restore to ingestion stream (rate-limited)
go run cmd/dlq/main.go -action=restore -dest=dlq_archive.bin -stream=ad:events -batch=1000 -rate=200

# Requeue DLQ to main stream
go run cmd/dlq/main.go -action=requeue -stream=ad:events:dlq -dest=ad:events -batch=1000 -rate=500
```

---

## Management API (selected endpoints)

### Campaign templates
- `POST /admin/campaign-templates`
- `GET /admin/campaign-templates`
- `POST /admin/campaign-templates/{id}/instantiate` (idempotency key)
- `POST /admin/campaigns/{id}/save-as-template`

### Delivery
- `POST /admin/campaigns/{id}/pause|resume|schedule`

### Brand creatives
- `POST|GET /admin/brands/{id}/creatives`
- `PUT|DELETE /admin/brands/{brand_id}/creatives/{id}`

### Payment
- `POST /admin/customers/{id}/payment-intent` (requires `PAYMENT_INTERNAL_TOKEN`)
- `POST /admin/customers/{id}/topup` (direct ledger credit, bypasses payment service)

### Billing
- `GET /admin/customers/{id}/billing` (requires `BILLING_INTERNAL_TOKEN`)
- `POST /admin/customers/{id}/billing/invoices` (requires `BILLING_INTERNAL_TOKEN`)

---

## Load testing and game day

Sharp-load validation (M6) uses k6 scripts and compose load-test profile.

| Script | Purpose |
| :--- | :--- |
| `scripts/load-test/run_spike_load.sh` | k6 spike: 200→2000 RPS, control-cohort p99 &lt; 80 ms |
| `scripts/load-test/run_dirty_load.sh` | Mixed dirty traffic baseline |
| `scripts/load-test/run_game_day.sh` | Scenarios A–H + `udp_severe` network profile |
| `scripts/load-test/snapshot_runtime.sh` | Capture tracker/processor/nginx runtime before/after |
| `scripts/load-test/analyze_bottlenecks.sh` | Generate bottleneck report from run dir |

```bash
docker compose -f docker-compose.load-test.yaml up -d   # optional pinned profile
bash scripts/load-test/run_game_day.sh check                # topology preflight
bash scripts/load-test/run_spike_load.sh
```

**Abort criteria:** control-cohort p99 &gt; 80 ms for 30 s, or R5 `AssertBudgetInvariant` violation. UDP recovery: [EDGE.md](./EDGE.md) Part III §1.

---

## CI (GitHub Actions)

Push to `main` only (no PR workflows). Lint, short tests, docker build, and `govulncheck` run locally — see `make check-local` and `make check-vuln`.

| Workflow | When | What |
| :--- | :--- | :--- |
| `ci.yaml` → `full-test` | push `main` | `scripts/ci/full_test.sh` |
| `ci.yaml` → `chaos` | push `main` | `scripts/chaos-drills/test_chaos.sh` (≥46 `chaos_proof` lines) |
| `perf-gate.yaml` | path-filtered push | smoke zero-alloc on github-hosted; strict benchstat when `PERF_RUNNER_LABEL` set |
| `perf-nightly.yaml` | Mon 03:00 UTC, manual | escape + redis/broker benchstat regression |
| `sentinel-chaos.yaml` | push `main`, manual | Sentinel failover script |

Set repository variable **`PERF_RUNNER_LABEL`** (e.g. `self-hosted`) to enable strict perf gate (benchstat vs baseline). Without it, `perf-gate.yaml` runs smoke mode only (zero-alloc, no CPU regression fail).

---

## Performance Gate

CI validates hot-path benchmarks on push when paths under `internal/ingestion/**`, `internal/rtb/**`, `internal/config/**`, `internal/database/redis*.go`, `pkg/logger/**`, `pkg/broker/**`, `deploy/nginx/lua/**`, or `api/**` change. Thresholds:

- Heap allocations: 0 allocs/op on gated benchmarks (CPU-only exempt list below)
- Memory: 0 B/op
- Latency regression: ≤12% (p < 0.05) — **strict mode only** (`PERF_RUNNER_LABEL` set in CI; local default)

On github-hosted runners without `PERF_RUNNER_LABEL`, CI runs **smoke mode**: zero-alloc check with 2 bench iterations, no benchstat baseline comparison (avoids flaky CPU failures).

Run locally before push: `make check-local` (lint, alloc gate, test, build). Hot-path perf vs baseline:

```bash
bash scripts/perf-gate/perf_gate_run.sh   # or: task perf-gate
make test-alloc-gate            # zero-alloc + fraud SLA in ./internal/ingestion/
```

Gated benchmarks (via `scripts/perf-gate/perf_gate_bench.sh`):

- Handler: `BenchmarkAdsPacketHandlerProto`, `Proto_NoExtra`, `Proto_ExtraBytes`
- Error paths: `BenchmarkHotPath_AdsPacketHandlerProto_reject404`, `_infra503` (infra: CPU-only)
- Micro: `BenchmarkHotPath_*` (timers, filter engine, latency ring, counters)
- Parse/routing: `BenchmarkTrackRequest_ParseJSON`, `BenchmarkCompositeRouting_Protobuf`

Excluded from gate: legacy `BenchmarkAdsPacketHandlerJSON`, `Proto_ExtraRepeated` (allocating repeated-field parse).

CPU-only exempt (alloc allowed, still benchstat CPU regression): `filterEngineCheck_withDeadline`, `AdsPacketHandlerProto_infra503`.

Nightly (`perf-nightly.yaml`, Monday 03:00 UTC): escape heap-line regression, Redis/broker benchstat regression (`--cpu-only`). Chaos runs in `ci.yaml` only (not duplicated in nightly).

PR also runs **`full-test`** job: `go test ./... -count=1` (no `-short`). Local: `make test-full`.

Perf runner: set repo variable `PERF_RUNNER_LABEL` (e.g. `self-hosted`) for `perf_gate` and nightly bench jobs.

Unit zero-alloc tests (in `test-alloc-gate`): `TestParseTrackRequestJSON_ZeroAlloc`, `TestAdEvent_UnmarshalVT_ZeroAlloc`, `TestComputeCompositeHashUUID_ZeroAlloc`, `TestFilterEngine_Check_zeroAlloc_fraudScoring`.

Escape analysis (nightly artifact or local):

```bash
bash scripts/perf-gate/escape_nightly_job.sh /tmp/espx-escape.txt
```

IDE settings (format on save, Go tools, debug env) live in Cursor user config (`~/.config/Cursor/User/settings.json` on Linux), not `.vscode/` in the repo. Use `make lint`, `task`, and lefthook for repeatable workflows.

---

## Post-deploy Redis Reconciliation

Run after rolling deploys that touch management outbox, Sentinel failover, or shard alignment fixes. Goal: confirm global keys are identical on all N shards and campaign-local keys sit on the shard `StaticSlotSharder` expects.

**When to run:**

- After deploy changing outbox handlers, `redis_global.go`, or sharder alignment
- After Sentinel failover or manual `redis_migrate_campaign.sh`
- Before closing a production change window

**Automated check:**

```bash
bash scripts/redis-ops/redis_reconcile_post_deploy.sh .env
```

Checks on every shard in `REDIS_ADDRS`:

| Key | Expectation |
| :--- | :--- |
| `config:version` | Same integer on all shards |
| `config:values` | Same `HLEN` on all shards |
| `blacklist:manual` | Same `SCARD` on all shards |

Exit code 1 prints drift details. Fix path:

1. Trigger settings sync: update any system setting in management UI or restart management (outbox cold sync on start).
2. For blacklist drift: re-apply block from management or replay outbox `UPDATE_BLACKLIST` rows.
3. For campaign budget drift: use campaign migration below.

**Campaign budget migration:**

Budget and pacing keys are shard-local. Tracker and outbox must agree on `StaticSlotSharder` (N=4).

```bash
# 1. Pause campaign in management
# 2. Migrate keys (auto-detects source shard from campaign UUID)
bash scripts/redis-ops/redis_migrate_campaign.sh <campaign_uuid> <source_shard> <target_shard>

# 3. Verify on target
redis-cli -h <target_host> -p <port> -a "$REDIS_PASSWORD" GET budget:campaign:<uuid>

# 4. Resume campaign; watch ad_budget_cache_miss_pg_total
```

Keys copied: `budget:campaign:{id}`, `campaign:settings:{id}`, `budget:daily_spent:campaign:{id}:*`.

Shard index helper:

```bash
go run ./scripts/redis-ops/campaign_shard.go <campaign_uuid> 4
```

**Alerts tied to this runbook:**

| Alert | Metric | Action |
| :--- | :--- | :--- |
| `ManagementOutboxLagHigh` | `ad_management_outbox_oldest_pending_seconds > 30` | Check management logs, Redis connectivity from outbox worker |
| `TrackerHealthDegraded` | `ad_tracker_health_degraded == 1` | `curl tracker:8181/health` — body `DEGRADED redis=0:0,...` |
| `TrackerRedisShardUnhealthy` | `ad_tracker_redis_shard_healthy{shard="X"} == 0` | Shard X down or Sentinel not promoted |

**Manual deep audit (optional):**

```bash
redis-cli -a "$REDIS_PASSWORD" -h host0 HGETALL config:values | sort > /tmp/s0.txt
redis-cli -a "$REDIS_PASSWORD" -h host5 HGETALL config:values | sort > /tmp/s5.txt
diff /tmp/s0.txt /tmp/s5.txt
```

For active campaigns, sample from Postgres:

```sql
SELECT id FROM campaigns WHERE status = 'ACTIVE' LIMIT 20;
```

For each id, `GET budget:campaign:{id}` only on shard from `go run ./scripts/redis-ops/campaign_shard.go {id}`.

---

## Redis Operations

### Topology verification

```bash
sh scripts/redis-ops/verify_redis_topology.sh .env
# Override count: REDIS_SHARD_COUNT=3 sh scripts/redis-ops/verify_redis_topology.sh .env
```

### Health checks

```bash
redis-cli -p 6479 -a "$REDIS_PASSWORD" PING
redis-cli -p 6479 INFO persistence | grep aof_enabled    # expect aof_enabled:1
redis-cli -p 6479 XLEN ad:events:stream
redis-cli -p 6479 XINFO GROUPS ad:events:stream
redis-cli -p 6479 XLEN ad:events:dlq
curl -s localhost:8181/health   # OK or DEGRADED redis=0:1,1:0,...
```

Tracker `/health` probes all shards every 2s in background. Status 503 when any shard unhealthy.

### TTC modes

| Mode | Env | Behavior |
| :--- | :--- | :--- |
| Fail-open (default) | `TTC_FAIL_CLOSED=false` | Click without `imp_ts` accepted; return code 10; `ad_ttc_bypass_total` increments |
| Fail-closed | `TTC_FAIL_CLOSED=true` | Click without `imp_ts` rejects as fraud (`missing_imp_ts`) |

Watch `ad_ttc_bypass_total` before enabling fail-closed. Alert `TTCBypassRateHigh` fires at >1% of `/track`.

Geo filter latency: `ad_filter_geo_duration_seconds` (sampled 1/128). Schedule/daypart stays in Go (`ScheduleFilter`).

### Sentinel failover testing

```bash
# Unit
go test ./internal/config/ -run Redis -count=1
go test ./internal/database/ -run ShardUniversal -count=1

# Stack (optional)
bash scripts/local-dev/dev_stack.sh sentinel
# Enable REDIS_SENTINEL_ADDRS in .env, restart tracker

# Scripted chaos
bash scripts/chaos-drills/sentinel_chaos_env.sh   # CI only; local: use your .env
bash scripts/chaos-drills/test_sentinel_failover.sh

# Manual chaos
docker stop redis-2
# Watch ad_redis_breaker_state{shard="2"} and /health on :8181
docker start redis-2
```

Breaker open timeout defaults to 5s (`REDIS_BREAKER_OPEN_TIMEOUT_MS`). Sentinel `down-after-milliseconds` is 5s; `failover-timeout` 10s. Expect breaker half-open within ~10-15s of clean failover.

---

## Redis Restart Runbook

**Trigger:** `SCRIPT FLUSH`, Redis restart, shard failover, volume loss, or TTL expiry on `budget:campaign:*` (24h).

**Symptoms:** `ad_redis_lua_noscript_total` >0, `ad_redis_lua_script_loaded` stale, `ad_budget_cache_miss_pg_total` >0.

### Planned maintenance order

1. Restart Redis shards; verify `PING` and AOF replay (`INFO persistence`).
2. Rolling restart trackers one at a time (30s drain between). Each runs `PreloadScripts` + `WarmFromRegistry`.
3. Verify:
   - `ad_redis_lua_script_loaded{shard}` == 1
   - `rate(ad_redis_lua_noscript_total[5m])` == 0
   - `rate(ad_budget_cache_miss_pg_total[5m])` == 0 under load

```bash
for t in tracker-0 tracker-1 tracker-2 tracker-3; do
  docker compose restart "$t"
  sleep 30
done
```

### Emergency recovery (no tracker restart)

**1. Manual SCRIPT LOAD on every shard**

```bash
LUA_FILE=internal/ingestion/filter/unified.lua
for port in 6479 6480 6481 6482; do
  sha=$(redis-cli -p "$port" -a "$REDIS_PASSWORD" --no-auth-warning SCRIPT LOAD "$(cat "$LUA_FILE")")
  redis-cli -p "$port" -a "$REDIS_PASSWORD" --no-auth-warning SCRIPT EXISTS "$sha"
done
```

**2. Trigger budget warm**

```bash
redis-cli -p 6479 -a "$REDIS_PASSWORD" --no-auth-warning \
  PUBLISH campaigns:update "00000000-0000-0000-0000-000000000001"
```

Or wait for `REGISTRY_SYNC_INTERVAL_MS` (default 60s).

**3. Verify**

```bash
curl -s localhost:8181/metrics | grep -E 'ad_redis_lua_noscript|ad_redis_lua_script_loaded|ad_budget_cache_miss'
```

Manual SCRIPT LOAD stops NOSCRIPT fallbacks but may not update `ad_redis_lua_script_loaded` gauge (set only at tracker startup). Prefer rolling restart when `RedisLuaScriptNotLoaded` alert fires.

### On-call decision tree

| Alert | Immediate | Proper fix |
| :--- | :--- | :--- |
| `RedisLuaNoScriptFallback` | Manual SCRIPT LOAD | Rolling restart trackers |
| `RedisLuaScriptNotLoaded` | Rolling restart trackers | Fix Redis connectivity |
| `BudgetCacheMissPG` | PUBLISH `campaigns:update` | Rolling restart if keys broadly missing |

Do not run `SCRIPT FLUSH` or `FLUSHDB` in production without a maintenance window.

---

## Multi-Shard Operability

### Shard down (blast radius)

- Symptom: `ad_redis_breaker_state{shard="X"} == 1`, or `/health` shows `DEGRADED`
- Effect: campaigns on shard X get 503 + `Retry-After: 1`. Other shards unaffected.
- Sentinel path: set `REDIS_SENTINEL_ADDRS`; Go services reconnect after promotion (~10–15s).
- Without Sentinel: wait for breaker half-open (5s) on transient failure; permanent loss requires key migration (below).

### Budget key migration

Budget keys are shard-local: `budget:campaign:{id}`, `budget:daily_spent:*`, fcap keys. Lua never crosses shards.

To move a campaign from shard S to T:

1. Pause campaign in management.
2. DUMP/RESTORE keys from S to T (preserve TTLs).
3. Verify: `redis-cli -h target GET budget:campaign:{id}`.
4. Resume campaign. Monitor `ad_budget_cache_miss_pg_total`.

Changing N (shard count) requires all clients (tracker, management, processor, Nginx Lua) to agree on new N simultaneously, plus full key migration. Use blue/green deploy. For frequent resize, evaluate `JumpHashSharder` (`go test ./internal/ingestion/ -run TestSharderRebalanceImpact -v`).

### StaticSlot vs JumpHash

| | StaticSlot | JumpHash |
| :--- | :--- | :--- |
| Remap on N change | ~100% | ~1/N |
| Hot-path cost | Lowest | Higher (float loop) |
| Production default | Yes | Tests / analysis only |

### Fixed N=4 policy

`ENV=production` enforces `len(REDIS_ADDRS) == 4`. Scale ingestion horizontally (more tracker replicas), not Redis shards, without a slot-migration project. Resize blast radius: ~67% campaigns remap on N change with StaticSlot (see [EDGE.md](./EDGE.md) Part III).

---

## Log Evacuation

Production image is distroless Go binary (`cmd/log-evacuator`). Uploads rotated segments to S3 with checkpoint persistence.

- Set `LOG_EVACUATOR_S3_BUCKET`, `LOG_EVACUATOR_S3_REGION` (or `AWS_REGION`), and AWS credentials in `.env`
- Optional: `LOG_EVACUATOR_S3_PREFIX`, `LOG_EVACUATOR_S3_ENDPOINT` (MinIO/localstack), `LOG_EVACUATOR_CHECKPOINT_PATH`
- Cron: `deploy/cron/log-evacuate.cron` (every 5min) or run as a long-lived service via compose profile `tools`
- Flow: tracker `pkg/logger` writes raw `.log`, async zstd + AES-GCM → `.log.zst.ready`; evacuator renames to `.log.zst.evacuating`, uploads to S3 with SHA-256 metadata + MD5 ETag verification, checkpoints, deletes local. Broker mmap segments (`pkg/broker/log`) are a separate uncompressed path.
- Stuck uploads: `.evacuating` files are retried on startup; failed uploads roll back to `.ready`

Profile `tools` in compose starts `log-evacuator`.

---

## Testing

```bash
make test-unit          # fast, -short
make test-int           # integration/e2e/chaos tests in tests/
make test-alloc-gate    # hot-path zero-alloc + fraud SLA (local check-local)
make test-full          # full suite, no -short (CI full-test)
make check-local        # lint + alloc gate + test + docker build
make check-vuln         # govulncheck (local, not CI)
make test-chaos         # scripts/chaos-drills/test_chaos.sh (Docker)
make test-sentinel-chaos
task test-full          # optional: race detector on ./... (not CI-equivalent)
bash scripts/local-dev/dev_preflight.sh   # after compose up
```

Redis-related tests:
- `internal/database/redis_shards_test.go` — direct vs Sentinel options
- `internal/config/redis_test.go` — production 6-shard enforcement
- `internal/ingestion/sharding_test.go` — StaticSlot vs JumpHash remap stats
- `internal/ingestion/unified_lua_test.go` — EVALSHA latency profile
- `internal/management/redis_global_test.go` — config replication
- `internal/ingestion/settings_test.go` — shard failover reads

---

## Verification Matrix

| Area | Command | Expectation |
| :--- | :--- | :--- |
| Sharder divergence | `go test ./internal/ingestion/ -run TestSharderStaticVsJumpHashDivergence` | PASS, log ~84% mismatch |
| Management integration | `go test ./internal/management/...` | PASS |
| Tenant isolation | `go test ./internal/management/... -run Isolation` | 403 |
| Redis outage auth | `go test ./internal/management/... -run MeRedisOutage` | 401 fail-closed |
| Outbox chaos | `go test ./internal/management/ -run Chaos` | PASS |
| Hot path perf | `task perf-gate` or `scripts/perf-gate/perf_gate_run.sh` | perf_gate CI |
| Payment | `go test ./internal/payment/...` | PASS |
| Billing | `go test ./internal/billing/...` | PASS |
| Notifier | `go test ./internal/notifier/...` | PASS |
| Config replication | `go test ./internal/management/ -run 'TestSyncGlobal\|TestBlockIP_Multiple'` | PASS |
| Settings failover | `go test ./internal/ingestion/ -run TestSettingsWatcher` | PASS |
| Redis shards | `go test ./internal/database/ -run ShardUniversal` | PASS |

Full suite (slow): `make test-full` or `go test ./... -count=1`

---

## Edge hardening (optional XDP)

OpenResty Lua handles L7 ingress on the default path. Optional XDP/eBPF L4 filter: `deploy/edge/xdp/bpf/edge_filter.c`, host tuning scripts in `scripts/edge-tuning/`.

Phase 0 ops: `edge_nic_tune.sh`, `edge_sysctl.sh`, `edge_baseline.sh` (point-in-time SLA snapshot).

SLA targets: tracker `ad_http_request_duration_seconds` p95 < 50 ms, p99 < 80 ms.

Rollback: revert nginx Lua/conf; detach XDP if deployed. See [EDGE.md](./EDGE.md) Part I §5.

---

## Infrastructure rationale (Compose, k3s, Terraform)

Hot path (gnet trackers, OpenResty) and cold path (auth, management, payment, processor, etc.) need different deployment models.

**Docker Compose (local data plane):** Postgres, ClickHouse, Redis ×4 on one host. Hot-path services use `network_mode: host` to avoid bridge NAT overhead. Compose does not split edge vs internal perimeter, lacks readiness orchestration across 7+ cold services, and scales poorly — so it is limited to **stateful infra locally**.

**k3s (application orchestration):** Declarative Deployments/Services/ConfigMaps/Secrets for cold path (`espx` namespace). Hot path in `espx-edge` with `hostNetwork: true` (trackers, OpenResty DaemonSet) — no CNI NAT on the ingest path. Local k3s matches staging topology (probes, DNS, rollout).

**Terraform:** Renders `deploy/k8s/*.yaml.tpl` via `templatefile()`, injects secrets from `terraform.tfvars` (gitignored), applies with kubectl provider. `k3s-install` module for local cluster lifecycle.

**Local wiring:** Compose exposes Postgres, ClickHouse, and Redis on host ports. Terraform applies cold-path manifests to k3s `espx`. Cold-path pods reach data stores via host ports; hot-path pods in `espx-edge` (`hostNetwork`) reach the same stores on loopback. Terraform does not manage Compose.

**Staging wiring:** Terraform applies manifests to a remote k3s cluster. Managed Postgres, ClickHouse, and Redis connect to cluster workloads via DSNs in `terraform.tfvars`.

| Task | Local | Staging / prod |
| :--- | :--- | :--- |
| Stateful stores | Compose | Managed Postgres, ClickHouse, Redis |
| Cold path | k3s `espx` | k3s / K8s |
| Hot path | k3s `espx-edge`, hostNetwork | Edge nodes, hostNetwork |
| Config deploy | `terraform apply` (local/staging) | CI/CD + Terraform |

```bash
bash scripts/k8s/k8s_cold_path_up.sh    # Compose DBs + cold path k3s
bash scripts/k8s/k8s_hot_path_up.sh     # hot path into espx-edge
bash scripts/k8s/k8s_staging_apply.sh     # staging
cd deploy/terraform/envs/local && terraform destroy
bash scripts/local-dev/dev_stack.sh down         # stop Compose volumes
```

---

## Testing pyramid (CI reference)

| Level | Scope | Examples |
| :---: | :--- | :--- |
| 4 | Chaos | testcontainers, SIGKILL, network fault |
| 3 | Integration | HTTP/gRPC e2e, real Postgres/Redis (no sqlmock) |
| 2 | Smoke | `check_deps.sh`, `verify_redis_topology.sh` |
| 1 | Unit | Table-driven tests, `t.Parallel()`, zero-alloc gate |

**Chaos gate:** successful scenarios emit `chaos_proof fault=<name> …`; `scripts/chaos-drills/test_chaos.sh` requires `CHAOS_MIN_PROOFS` unique proofs (default 46) or CI fails.

**Perf gate (`perf-gate.yaml`):** on hot-path diffs — hosted runners smoke-check `0 allocs/op`; self-hosted runs `stabilize_cpu.sh` + `benchstat` and blocks &gt;5% regression.

**Compiler checks (gcflags):** escape analysis (`-gcflags="-m -m"`), inlining (`-m`), BCE (`-d=ssa/prove/debug=1`) on `internal/…` — heap escapes and bounds checks in hot loops fail the gate.

---

## Known limitations

- Stripe checkout: `createStripeCheckoutSession` returns `ErrProviderNotConfigured` even with `STRIPE_SECRET_KEY` set; use unset key + `MockProvider` for local dev. See [Stripe checkout policy](#stripe-checkout-policy).
- Migration `00022_campaign_delivery_features.sql` may lack goose markers; verify applied manually if templates/creatives tables missing.
- `broker`, `log-shipper`, `dlq`, `admin` are buildable but outside default compose.
- Billing and notifier schemas are not auto-applied with ads migrations; run their goose Up SQL when enabling those services.
