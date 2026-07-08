# MILESTONE: Control Plane eSPX

Roadmap for cold path improvements: `cmd/management`, settlement gRPC, `cmd/payment`, `cmd/billing`, `cmd/auth`, `cmd/notifier`, `cmd/ivt-detector`, workers processor. Hot path (`/track`, gnet, `unified_filter.lua`) is out of scope, except propagation contracts: PG outbox -> Redis keys -> `campaigns:update` pubsub -> in-memory registry in tracker.

Testing standards - [GUIDE_CHAOS_RELIABILITY_RU.md](../GUIDE_CHAOS_RELIABILITY_RU.md). Service boundaries - [GUIDE_IDEAS_MICROSERVICES_RU.md](../GUIDE_IDEAS_MICROSERVICES_RU.md).

## Current topology

`cmd/management/main.go` runs HTTP `:8188` (`cfg.ManagementPort`), settlement gRPC `:51053` (`cfg.SettlementServerPort`), and gRPC clients to auth/payment/billing/notifier. `management.NewService` starts workers: `OutboxWorker` (poll 20 ms), `CampaignDrainWorker`, `CreditScoringWorker` (24 h), `ScheduleWorker`, `RunSystemStateSyncer`. In `main` additionally: `ads.SyncWorker` for each Redis shard, `ReconWorker`, `PacingControllerWorker`, `BlacklistJanitor`, and optionally `NginxConfigWorker`, `SlotMigrationOrchestrator`, `OpsAlerter`.

`cmd/payment` - gRPC `:51052`, webhooks HTTP `:8187`, schema `payment.*`, `OutboxWorker` poll 100 ms -> settlement gRPC `ApplyPaymentCredit` / refund / chargeback RPC from `api/settlement.proto`.

`cmd/billing` - gRPC `:51054`, reads `balance_ledger` via billing schema. `cmd/notifier` - gRPC `:8085`, async delivery. `cmd/ivt-detector` - CH batch -> HTTP `POST /admin/blacklist` with `ADMIN_API_KEY` (replace with gRPC, M7.10).

Postgres - single instance, schemas `payment`, `billing`, `notifier`; ads tables (`campaigns`, `balance_ledger`, `outbox_events`) in public/ads. Redis - N standalone masters, sharding `ads.StaticSlotSharder` by `campaign_id`.

Write pattern: mutation in PG TX -> `outbox_events` -> worker applies side effect in Redis -> pubsub invalidation. Money: truth in `balance_ledger`; Redis `budget:{campaign_id}` - cache for Lua, reconciled by `ReconWorker` and `SyncWorker`.

Veto from guide: no gRPC/HTTP to payment/notifier from `processTrack()`; new `cmd/*` only when score >= 11; node-local I/O - separate goroutine via `Service.StartBackgroundWorker`, not HTTP pool management.

## Outbox and metrics

Table `outbox_events`: `status` PENDING -> PROCESSING -> PROCESSED; claimed via `GetPendingOutboxEventsForUpdate` in `internal/ads/queries/management.sql` (`ORDER BY created_at ASC`, `FOR UPDATE SKIP LOCKED`, batch up to 1000). Handlers in `internal/management/outbox_handlers.go`: `CREATE_CAMPAIGN`, `PAUSE_CAMPAIGN`, `RESUME_CAMPAIGN`, `UPDATE_CAMPAIGN_SCHEDULE`, `SYNC_BRAND_CREATIVES`, `CANCEL_CAMPAIGN`, `UPDATE_CAMPAIGN_PACING`, `UPDATE_CAMPAIGN_FRAUD`, `UPDATE_SETTINGS`, `UPDATE_BLACKLIST`, `CONFIGURE_BRAND_FCAP`.

Payment outbox - `payment.payment_outbox`, event `SETTLE_BALANCE`; payload `SettleBalancePayload` in `internal/payment/outbox_worker.go`.

Lag metrics: `internal/management/outbox_metrics.go` -> `ad_management_outbox_oldest_pending_seconds`. Ops: `RegisterOpsRoutes` in `internal/management/ops.go` - `/health`, `/metrics`.

## Testing

Chaos testing is mandatory for outbox, ledger, quota, and RTB admin -> catalog reload, consent -> hot path. Each test must have one `chaos_proof fault=...`, one invariant, and one failure class (R9, R10). Money/outbox paths: >= 20 goroutines, testcontainers PG+Redis, no sqlmock. Management outbox races: 4 workers (`TestChaos_DualOutboxWorkerRace`). Payment webhook dedup: 20 workers per single `event_id`. Budget invariant R5: `(budget_limit - redis_remaining) = pg_current_spend + sync_delta` (+/- 1 micro), `AssertBudgetInvariant` in chaos suite.

## Distributed systems constraints

Failure model: crash-recovery (omission, duplication), non-Byzantine. Webhook/Stripe - at-least-once delivery; idempotency based on `ledger_idempotency_key` and `payment_intent_id` in `Service.ApplyPaymentCredit` (`internal/management/service.go`).

Effectively-once: outbox relay + idempotent consumers. Do not introduce 2PC between payment <-> management <-> billing.

`balance_ledger` / settlement - single PG TX, serializable. Budget on hot path - Lua on a single Redis shard, linearizable per key. PG `current_spend` <-> Redis - eventual, `ReconWorker.ReconcileWindow` (lag window 2 h) + quota recon every 10 s when `QUOTA_MODE=shadow|live`. Global config/blacklist - `UPDATE_SETTINGS` / `UPDATE_BLACKLIST` -> `pkg/cold` replicate to all shards, version key `config:version`.

Slot migration cutover (`internal/management/service_slot_migration.go`, routes in `handler_slot_map.go`): `EnsureSlotMigrationJobs` -> `CopySlotMigrationData` (checkpoint per campaign in PG) -> `activateSlotMapVersion` -> trackers reload registry. Rollback: `rollbackSlotMap` without losing ledger rows.

M5.0 `DeliveryOptimizerWorker` (new file, e.g. `internal/management/delivery_optimizer_worker.go`): single tick, merge inputs from autoscale (`service_autoscaling.go`), pacing (`service_pacing.go` / `ClosedLoopPacingController`), MAB, bid floor; <= 1 outbox event / campaign / tick; priority of side effects: `PAUSE` > `UPDATE_CAMPAIGN_PACING` > `SYNC_BRAND_CREATIVES`.

M6.4 erasure: table `privacy_erasure_requests`, tombstone -> PG anonymize PII -> outbox `PURGE_USER_DATA` (new event type + handler) -> CH mutation by `user_id` hash -> verify job. Retry idempotent.

M7.1 invoice cron: `pg_advisory_lock(hashtext(customer_id::text || billing_month))` or `INSERT ... ON CONFLICT` + `SKIP LOCKED` claim in billing worker.

## Phase M0 - WIP and ops [implemented]

**M0.1 QuotaManager.** Code: `internal/management/quota_manager.go`. Poll `budget:refill_needed` via `SPopN` on each shard, refill via `ads.QuotaRepo`. Env: `QUOTA_MODE=off|shadow|live` (default `off`, `internal/config/env.go`), `QUOTA_CHUNK_SIZE`, `QUOTA_REFILL_THRESHOLD_PCT`. Not connected in `cmd/management/main.go` - add `svc.StartBackgroundWorker(func() { NewQuotaManager(svc).Start(ctx) })`. Shadow: refill is logged, Redis budget keys are not modified. Live: PG `reserved_amount` + Redis quota keys. Recon quota: `ReconWorker.ReconcileQuotas` is already called under shadow/live. Chaos: `quota_refill_race`, `quota_dead_shard_release`.

**M0.2 AutoscaleBudgets.** Code: `internal/management/service_autoscaling.go` - implement `GetAllActiveCampaignsWithStats`, budget shift between campaigns of the same customer, write ledger entries `FREEZE`/`RELEASE`, and enqueue outbox `CREATE_CAMPAIGN` to update Redis budget. Env variables: `AUTOSCALE_HIGH_CTR_THRESHOLD`, `AUTOSCALE_LOW_CTR_THRESHOLD`, `AUTOSCALE_MIN_IMPRESSIONS`, `AUTOSCALE_SHIFT_AMOUNT`, `AUTOSCALE_MIN_REMAINING_BUDGET`. No scheduled worker exists: add a ticker based on `AUTOSCALE_INTERVAL_MS` (0 = off) in `main` or delegate to M5.0 DeliveryOptimizerWorker. Before each tick, call `syncWorkers[].SyncAll`.

**M0.3 Payment financial recon -> notifier.** Code: `internal/payment/recon_service.go`, tables `payment.financial_recon_runs` / `payment.financial_recon_findings` (`migrations/00005_payment_financial_recon.sql`). Env: `PAYMENT_FINANCIAL_RECON_INTERVAL_MS` (0 = off). After `Run`: if finding severity is >= WARN, invoke `OpsAlerter` or direct `NotifierClient.SendNotification` gRPC call. Handle cooldown check via `OpsAlerter.shouldSend`.

**M0.4 Management recon -> ops.** Code: `internal/management/recon_service.go`, table `recon_discrepancies`. For any unresolved discrepancy older than 1 hour, trigger `OpsAlerter.AlertReconDiscrepancy` (add this method analogously to `AlertOutboxStuck`). Auto-adjust: `ReconcileWindow` already writes `RECONCILIATION_ADJUST` to the ledger when |delta| <= chunk size.

**M0.5 Compose prod profile.** Enable `PAYMENT_FINANCIAL_RECON_INTERVAL_MS=3600000` in the production docker-compose profile; monitor finding kinds `DEAD_OUTBOX`, `MISSING_LEDGER_TOPUP` (detailed in `docs/reports/PAYMENT_FINANCIAL_RECON.md`).

**M0.6 Stripe policy.** Code: `internal/payment/provider_stripe.go` - currently a placeholder `createStripeCheckoutSession`; `NewStripeProvider` delegates there. Document in `docs/development.md`: this remains mock-only until M4.3 or live DoD verification is completed.

**M0.7 Outbox priority lanes.** Currently, `GetPendingOutboxEventsForUpdate` sorts only by `created_at ASC`. Modify the SQL query or implement a two-phase claim: first claim `event_type IN ('UPDATE_BLACKLIST','PAUSE_CAMPAIGN','CANCEL_CAMPAIGN')` with a limit, then fetch remaining events. Alternative: add a `priority SMALLINT` column and order by `priority DESC, created_at ASC`. Chaos test: `outbox_priority_lanes` - enqueue 500 `UPDATE_CAMPAIGN_PACING` and 1 `UPDATE_BLACKLIST`, verify that the blacklist event is PROCESSED first.

**M0.8 SETTLEMENT_FAILED -> notifier.** On permanent settlement failure in the payment outbox, enqueue a notification to the notifier via `OpsAlerter` or a payment-side hook. Deduplicate using `payment_intent_id` with a cooldown of 300 seconds (`OPS_ALERT_COOLDOWN_SEC`).

**M0.9 Settlement gRPC metrics.** Add `grpc.UnaryServerInterceptor` to `settleGRPC` in `cmd/management/main.go`: export histogram `settlement_grpc_request_duration_seconds` and counter `settlement_grpc_errors_total`. Configure Prometheus alert rules accordingly.

**M0.10 Notifier rate limit.** Implement a token bucket rate limiter in `cmd/notifier` per recipient/provider; handle Telegram API 429 response by applying backoff. Env: `NOTIFIER_TELEGRAM_RATE_LIMIT` (default 20/min).

**M0.11 Shard health endpoint.** Create a new handler `GET /admin/ops/shards` requiring `PermShardsRead` permission. It must execute: per-shard Redis `PING`, retrieve circuit breaker state from settings, calculate outbox lag estimate, and query `GET config:version` on each shard comparing it against the last processed outbox event ID from PostgreSQL. Reuse components from `internal/management/redis_global.go` and `service_system.go`.

Chaos M0: implement tests for `quota_dead_shard_release`, `autoscale_no_double_freeze`, `financial_recon_ops_alert`, `outbox_priority_lanes`, and `settlement_failed_notifier`. Document the new `chaos_proof` validations under `docs/reports/`. **Done:** see [CHAOS_M0.md](./reports/CHAOS_M0.md). **M0 summary (RU):** [MILESTONE_M0_RU.md](./reports/MILESTONE_M0_RU.md).

## Phase M1 - Reporting & Read API [implemented]

Introduce a new router prefix `/api/v1/*` next to `/admin/*` in `handler.go` or within a separate `handler_api.go` file. Authentication: reuse `AuthMiddleware` and RBAC permissions from `permissions.go`; enforce tenant isolation via `AuthenticatedUser.CustomerID` for role `U` (defined in `rbac.go`).

**M1.1** `GET /api/v1/campaigns/{id}/stats` - returns aggregates from Postgres (`campaigns.current_spend`, metrics counters) merged with ClickHouse hourly Materialized Views (create `mv_campaign_hourly_*` in processor migrations). Support query parameters `from`, `to`, and `granularity=hour`. If ClickHouse replication lag exceeds 5 minutes, include `stale: true` in the JSON response payload.

**M1.2** `GET /api/v1/customers/{id}/balance` - returns `customers.balance` and executes `SELECT ... FROM balance_ledger ORDER BY id DESC LIMIT 100`. Prevent cross-customer access by returning HTTP 403 if `user.CustomerID != id`.

**M1.3** `GET /api/v1/recon/runs` - returns a union of management `recon_runs` and payment `financial_recon_runs` tables, with a query filter `service=management|payment`.

**M1.4** CSV export `GET .../export?format=csv` - stream `text/csv` with a maximum limit of 10 MB. Apply a rate limit per customer (extend `ipRateLimiter` or use a per-API-key limiter).

**M1.5** Billing Prometheus - expose HTTP endpoint `:9092/metrics` in `cmd/billing` or register metrics inside the existing application metrics registry.

**M1.6** Materialized views/indexes for `GET /admin/audit` - add a migration with `CREATE INDEX ON audit_log(created_at DESC)` or construct a materialized view for listing entries; generate a new sqlc query `ListAuditPaginated`.

**M1.7** `GetLedgerEntry` RPC in `api/settlement.proto` - perform lookup by `payment_intent_id`, implement the handler in `settlement_handler.go`, and invoke it from the payment service gRPC client instead of executing direct SQL queries.

**M1.8** Audit export worker - launch via `StartBackgroundWorker` to write a daily dump of `audit_log` to `./data/audit-export/YYYY-MM-DD.csv`. Use configurations: `AUDIT_EXPORT_PATH` and `AUDIT_EXPORT_RETENTION_DAYS=90`. Follow the same implementation pattern as `NginxConfigWorker`.

Pagination: enforce `limit` max 1000, default 50. Error responses must use `pkg/httpresponse` JSON formatting `{"code","message"}`.

Chaos M1: write validation tests for `api_tenant_isolation`, `api_ch_lag_stale_ok`, and `ledger_export_cursor`.

## Phase M2 - Supply chain (IAB) [implemented]

**M2.1** Database migration: create the `sellers` table containing columns: `seller_id`, `domain`, `seller_type`, `name`, and `is_confidential`. Set up handler `GET /.well-known/sellers.json` (or map via an Nginx alias pointing to management static assets); conform to the IAB sellers.json Final 2019 JSON schema specification. Implement in-memory caching with a TTL of 60 seconds.

**M2.2** Database table `ads_txt_entries`; implement export endpoint `GET /admin/supply/ads.txt` returning plain text in ads.txt 1.1 format (including `OWNERDOMAIN` and `MANAGERDOMAIN`).

**M2.3** Add database column `campaigns.supply_chain_nodes JSONB` (restricted to a maximum of 10 hops); record mutations inside the audit log by triggering the existing `AuditLog` utility.

**M2.4** Admin CRUD endpoints `POST/GET/PUT/DELETE /admin/supply/sellers` and `/admin/supply/ads-txt`; enforce `settings:write` permission via RBAC.

Publish mechanism: enqueue outbox event `UPDATE_SUPPLY_FILES` (define a new event type) - triggers an Nginx configuration reload or updates files directly in the export path. Chaos test cases: `sellers_json_invalid`, `supply_outbox_redelivery`. See `docs/reports/CHAOS_M2.md`.

## Phase M3 - OpenRTB Control Plane [implemented]

Hot path components: `internal/rtb/` (`catalog_registry.go`, `budget_store.go`, `auction.go`), ingestion hook `internal/ads/rtb_track.go`, validation parser `internal/ads/openrtb_parse.go` (`ParseOpenRTB3Payload`). Environment setting: `RTB_MODE=off|shadow|live` (`internal/config/rtb.go`).

**M3.1** Database migration `rtb_deals` (with columns: `deal_id`, `floor_micro`, `geo_mask`, `cat_mask`, `pacing`, and `customer_id`). Build admin CRUD endpoints `POST/GET/PUT/DELETE /admin/rtb/deals` that write to the outbox event `RELOAD_RTB_CATALOG`, which propagates to trigger tracker `Registry.UpdateCampaigns` and rebuild the deal index.

**M3.2** `POST /admin/rtb/validate-bid-request` - parse OpenRTB 2.6 payloads via cold path JSON parsing, returning a JSON body `{valid, errors[]}` capped at 50 errors. Reuse code validation logic from `openrtb_parse.go` where appropriate.

**M3.3** OpenRTB 3.0 linting - validate matching fields with `ParseOpenRTB3Payload`; reject payloads if `cur` is not USD or EUR.

**M3.4** `GET /admin/rtb/shadow-diff?window=1h` - compare shadow evaluation outcomes against live would-bid decisions retrieved from ClickHouse or in-memory tracking counters; verify metrics using golden test fixtures under `internal/rtb/chaos_*`.

**M3.5** System setting `RTB_BUDGET_AUTHORITY=rtb|lua` in `system_settings` propagating via outbox event `UPDATE_SETTINGS`; if configured to `lua`, budget authority logic remains native to `unified_filter.lua`.

**M3.6** Postgres deals data schema: unique constraint on `deal_id`, `bidfloor` stored as int64 micro-units, enforce a minimum of 1 seat.

**M3.7** Bid floor optimizer - query ClickHouse for win/loss rate percentages, feed these recommendations into DeliveryOptimizerWorker, write output to Redis keys `rtb:floor:{deal_id}`, which trackers read in a read-only fashion.

**M3.8** `POST /admin/campaigns/{id}/warm-budget` - query Postgres for the difference `budget_limit - current_spend` and execute `setCampaignBudgetRemaining` directly without forcing a full registry restart; reuse existing outbox worker helper functions.

Chaos M3: extend tests in `internal/rtb/chaos_*` with scenarios: `rtb_catalog_reload`, `rtb_shadow_live_parity`, and `rtb_deal_floor`. **Done:** see [CHAOS_M3.md](./reports/CHAOS_M3.md).

## Phase M4 - Self-Serve API

Role `U` (`rbac.go`) already possesses permissions `campaigns:write` and `customers:read`. Expand the authorization middleware to perform scope checks verifying the `customer_id` context inside the request path or JSON request body.

**M4.1** `/api/v1/selfserve/campaigns` - provide a subset of `createCampaign`, pause, and resume endpoints from `handler.go`; enforce a budget headroom check `balance + overdraft >= budget_limit` before executing the database Postgres insert statement.

**M4.2** `POST /api/v1/selfserve/payment-intents` - proxy request to `PaymentClient.CreatePaymentIntent`, enforcing that the `Idempotency-Key` HTTP header is present in the request.

**M4.3** Stripe live: integrate `stripe-go` within `createStripeCheckoutSession`, set up the webhook receiver at `internal/payment/http_webhook.go`, handle 3DS redirection return URL flow; keep operations out of PCI scope by ensuring no PAN numbers are stored in local databases.

**M4.4** `GET /api/v1/selfserve/invoices` - call `BillingClient` gRPC methods configured in `handler_billing.go`.

**M4.5** API keys: call the `auth.CreateAPIKey` gRPC endpoint requesting `campaigns:write` scope; enforce a rate limit of 30 RPS per API key in middleware logic.

**M4.6** `POST /admin/payment/webhooks/replay` - read JSON from `payment.webhook_events.payload_redacted` and execute processing pipeline with absolute idempotency; guarantee zero double balance credit events.

**M4.7** `GET /api/v1/disputes` - list `payment_intents` with `status = DISPUTED`, include linked `PAYMENT_CHARGEBACK` ledger entry IDs (implement in `internal/payment/dispute.go`, schema from `00004_payment_disputes.sql`).

Limit parameters in config/env: `SELF_SERVE_MAX_ACTIVE_CAMPAIGNS=500`, `SELF_SERVE_MAX_CREATES_PER_DAY=50`, configure min/max values for budget micro-units.

Chaos M4: write automated tests validating scenarios: `selfserve_overdraft_reject`, `selfserve_idempotent_create`, and `stripe_checkout_settlement`.

## Phase M5 - Forecasting & Pacing

Prerequisite: M5.0 DeliveryOptimizerWorker must be fully operational before enabling M0.2 autoscale, M5.6 smart pacing, M5.7 MAB, and M3.7 bid floor.

**M5.0** New background worker: configure tick interval via env `DELIVERY_OPTIMIZER_INTERVAL_MS`; aggregate recommendations in a Postgres temporary table or an in-memory map, perform a single merge pass, and execute exactly one outbox batch per campaign.

**M5.1** `POST /api/v1/forecast/campaign` - input parameters: budget, geo targeting, daypart window restrictions, active dates; output result: `impressions_p50/p90` estimates and `spend_curve[]` distribution.

**M5.2** Query ClickHouse `mv_campaign_hourly_*` using a lookback historical window of 90 days; flag the response with `low_confidence: true` if the total sample is under 1000 recorded impressions.

**M5.3** Enforce a ClickHouse query timeout threshold of 1.5 seconds; configure handler context deadline at 2 seconds, mapping timeouts to HTTP 503 Service Unavailable containing `retry_after`.

**M5.4** Advisory warning: if pacing is set to `EVEN` and estimated forecast underfill exceeds 20%, suggest switching to `ASAP` inside the JSON response payload only (do not switch automatically).

**M5.5** Optional: create `api/forecast.proto` definition and set up a matching gRPC server listener within the management daemon.

**M5.6** Smart pacing - extend the `ClosedLoopPacingController` logic in `pacing_controller_worker.go` to compute daypart weights using historical ClickHouse records instead of hardcoded linear EVEN pacing; route all execution outputs through the M5.0 framework.

**M5.7** Multi-Armed Bandit (MAB) - fetch CTR statistics grouped by `creative_id` from ClickHouse and store resulting weights inside the `brand:creatives` Redis hash; propagate updates via the `SYNC_BRAND_CREATIVES` outbox event; schedule this task with a 15-minute tick interval, requiring at least 1000 impressions per creative.

**M5.8** Dynamic overdraft - extend `CreditScoringWorker.calculateOverdraft` calculations to incorporate PG-Redis replication lag metrics derived from `ReconWorker`; trigger campaign suspension alerts only when `balance + overdraft < 0` is reached.

Chaos M5: write tests checking `delivery_optimizer_single_writer`, `forecast_ch_timeout` handling, and `forecast_deterministic` outcomes.

## Phase M6 - Privacy & Consent

**M6.1** Database schema migration: add `consent_events` (with columns: `user_id_hash BYTEA`, `purposes BIT(16)`, `source`, `created_at`); schedule a database retention cleaner job at 13 months.

**M6.2** `POST /api/v1/consent` - authorize request body payloads signed using HMAC-SHA256 and a configured shared secret key; map matching target purposes to `ad_storage` and `analytics_storage` status flags in Postgres.

**M6.3** Schema column `campaigns.require_consent_purposes BIT(16)`; enforce a hot-path verification inside the Go request filter before proceeding to Lua execution (introduce this configuration inside the campaign registry struct); if verification fails, reject request returning HTTP 204 with campaign budget left unmodified; data propagation: outbox event coupled with Redis Pub/Sub, achieving read-your-writes consistency within 2 seconds.

**M6.4** Erasure protocol (see system constraints): manage `privacy_erasure_requests` state transitions PENDING -> PG_ANONYMIZED -> REDIS_PURGED -> CH_PURGED -> COMPLETED.

**M6.5** `GET /admin/audit?redact_pii=true` - mask user email and IP addresses inside the JSON structures of `audit_log.payload`.

Chaos M6: write automated validation scripts verifying `consent_webhook_replay`, `consent_read_your_writes`, `require_consent_blocks`, and `erasure_partial_shard_failure`.

## Phase M7 - Billing, Notifier, IVT, Platform

**M7.1** Monthly cron inside `cmd/billing` or via `billing.StartInvoiceWorker` - trigger on the 1st of the month at 00:15 UTC; synchronize runs via `pg_advisory_lock`; execute the idempotent process `GenerateInvoice(customer_id, month)`.

**M7.2** Execute `GenerateInvoice` to render a PDF document (import a rendering package within the billing codebase) and deliver it using `notifier.SendNotification` including an attachment URL link or a secure presigned access token.

**M7.3** Credit notes - allow negative line item values mapping to corresponding database ledger types `PAYMENT_REFUND` and `RECONCILIATION_ADJUST`.

**M7.4** Multi-currency support - aggregate ledger records grouping by the database `currency` column; remove hardcoded references to USD inside `handler_billing.go` and billing gRPC responses.

**M7.5** On a `CheckLedgerBalanceInvariant` check failure, trigger an alert via `OpsAlerter` (follow the alerting structure used inside the billing chaos suite).

**M7.6** Alertmanager webhook receiver is already defined in `alertmanager_webhook.go`; deprecate direct notifications via `cmd/telegram` channels.

**M7.7** Template registry - create table `notifier_templates` (with columns: `name`, `body`, `vars`); modify the management client to deliver `template_id` and a variables map payload instead of sending raw HTML message strings.

**M7.8** `POST /admin/notifications/{id}/retry` - reset the matching FAILED database state inside the notifier schema; set up a retention job cleaning FAILED logs older than 7 days.

**M7.9** Optional: define and implement a batch endpoint `SendNotificationBatch` inside `api/notifier.proto`.

**M7.10** IVT detector integration - expose gRPC `BlockIP` as an internal API to replace HTTP webhooks protected with `ADMIN_API_KEY` (currently `cmd/ivt-detector/main.go` calls the management HTTP route directly). The handler must invoke `Service.BlockIP` which generates an outbox event `UPDATE_BLACKLIST`.

**M7.11** ASN datacenter classification rule in `internal/ivtdetector/analyzer.go` - perform ClickHouse query analysis supplemented with GeoIP ASN metadata enrichment.

**M7.12** Campaign-level CTR spike detection rule - aggregate metrics grouping by `campaign_id` in the ClickHouse verification queries.

**M7.13** Metrics instrumentation inside the ivt-detector daemon: export `ivt_candidates_total`, `ivt_enqueued_total`, and `ivt_backpressure_drops_total`.

**M7.14** `SuspiciousFinder` interface registry - register detection rules dynamically as plugin modules instead of modifying the core loops in `analyzer.go`.

**M7.15** Configure ClickHouse user profile `ivt_readonly` with restricted `GRANT SELECT` access permissions exclusively on metrics/analytics tables.

**M7.16** `BatchApplySettlement` RPC inside `settlement.proto` - enable efficient draining of the payment outbox after management service outages; process operations in batches where the size is <= 500 rows per request.

**M7.17** Database renaming `ad_event_processor` -> `espx` - prepare the migration script and compile a blue/green operational runbook using `pg_dump`.

**M7.18** Per-service `x-internal-token` authorization headers: configure `SETTLEMENT_INTERNAL_TOKEN`, `PAYMENT_INTERNAL_TOKEN`, etc.; add token rotation instructions to `development.md`.

**M7.19** Set up docker-compose profile `chaos` - coordinate payment, management, ivt, and notifier services inside the `scripts/test_chaos.sh` execution matrix.

**M7.20** Slot migration execution hook - update `SlotMigrationOrchestrator` (`slot_migration_orchestrator.go`) so that on process error or completion it calls `OpsAlerter.AlertSlotMigrationError`; perform post-cutover verification of the R5 invariant against sample campaigns for each Redis shard.

Chaos M7: write validation checks for `invoice_cron_idempotent`, `ivt_grpc_block_ip`, `batch_settlement_drain`, and `slot_migration_cutover_invariant`.

## Phase dependencies

M0 addresses disconnected WIP items (quota management, autoscaling, reconciliation alerts, outbox prioritization lanes) - establishing the base infrastructure for subsequent phases. M1 introduces the read API endpoints - which are prerequisites for M4 self-serve features and external partner integrations. M2 is independent of M3. M3 RTB admin depends on stable outbox propagation pipelines (completed in M0.7). M4 is built on top of M1 auth/RBAC capabilities and the payment/settlement pipelines. M5.0 DeliveryOptimizerWorker prevents concurrent execution issues between M0.2 autoscale, M5.6-M5.7, and M3.7 without race conditions. M6 consent propagation relies on the outbox and Pub/Sub mechanics. M7 billing/notifier/IVT tasks can proceed in parallel with M4-M6 after M0 operations hooks are completed.

## Out of scope

Hot path zero-allocation checking (`make test-alloc-gate`); Prebid.js integration; UID2/clean room integrations; mobile SDK/VAST/OM SDK; machine learning bidding models beyond simple MAB weights; `cmd/fraud-intelligence` real-time gRPC server; splitting `cmd/ledger` out into a separate microservice before an organizational trigger is reached.

## References

[Architecture](./architecture.md), [Development](./development.md), [GUIDE_CHAOS_RELIABILITY_RU](../GUIDE_CHAOS_RELIABILITY_RU.md), [GUIDE_IDEAS_MICROSERVICES_RU](../GUIDE_IDEAS_MICROSERVICES_RU.md), [Kleppmann DDIA Ch. 9](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/ch09.html), [Logs not dual writes](https://martin.kleppmann.com/2015/05/27/logs-for-data-infrastructure.html), [Tanenbaum DS3](https://www.distributed-systems.net/index.php/books/ds3/), [MANAGEMENT_SEPARATION](./reports/MANAGEMENT_SEPARATION.md), [PAYMENT_FINANCIAL_RECON](./reports/PAYMENT_FINANCIAL_RECON.md), [PAYMENT_PAYBACK](./reports/PAYMENT_PAYBACK.md), [PAYMENT_CHARGEBACK](./reports/PAYMENT_CHARGEBACK.md).
