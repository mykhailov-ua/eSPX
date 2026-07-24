# Code Style

Repository layout and review rules. Hot path overrides: [GO.md](./GO.md), `.cursorrules`.

---

## Tactical Architecture

### Architectural Approach

eSPX does **not** use clean or hexagonal architecture. There are no package trees like `entity/`, `usecase/`, `repository/`, `ports/`, or `adapters/`. There are no parallel struct families for a single database table (e.g., database model, domain entity, and API representation). There are no mapper registries or reflection-based mapping solutions.

These patterns increase heap churn and lead to out-of-sync fields where Postgres rows, Redis replicas, registry snapshots, and dashboard JSON must remain strictly consistent. We use **one flat Go package per service**, **files acting as modules** (R2), **struct names and tags explicitly declaring their transport role**, and **mapping strictly at I/O boundaries** (in a single step/hop).

### Repository Directory Tree

```text
api/                      # Protobuf sources (buf -> internal/<svc>/pb/)
cmd/<binary>/main.go      # Config, pools, constructors — no business logic (R4)
internal/
  domain/                 # Shared hot-path types and repository interfaces
  config/                 # Environment and runtime configuration
  database/               # pgx / Redis shard helpers
  metrics/                # Prometheus collectors
  <service>/              # Flat application package (R1)
    *.go                  # All application logic in the package root
    db/                   # sqlc output (package db)
    queries/              # sqlc input (.sql)
    migrations/           # goose SQL migrations
    pb/                   # vtproto / gRPC generated code (package pb)
pkg/                      # Utility helpers — no domain imports
deploy/                   # nginx, compose, operator resources
```

Root service folders contain non-Go resources bound to that binary (e.g., `unified-filter.lua` in ads). Generated code must reside only in the allowed subdirectories (R1).

### What We Avoid

1. **Nested packages like `filter/`, `ingest/`, `repo/`, `service/`** — import cycles and navigation complexity; use file name prefixes instead (R2).
2. **"Entity + DB Model + API Model" sets for every aggregate** — manual mapping overhead; fields go out of sync across the three copies.
3. **Repository interfaces for every table in a separate package** — sqlc's `db.Queries` is a repository in itself; an extra layer of indirection adds no value.
4. **Generic mappers like `MapStruct` or reflection-based solutions** — hidden allocations; incompatible with hot-path zero-alloc rules.
5. **Use-case structs that only forward calls to another level** — unnecessary step; call `Service` methods directly.

### File Roles (Not Layers)

Code inside `internal/<service>/` is grouped by **file name**, not by package nesting:

1. **Transport Entry Point** — `handler.go`, `handler_<area>.go`, `http_<area>.go`. Decodes requests, calls the service, writes responses.
2. **Core Logic** — `service.go`, `service_<domain>.go`, `track_core.go`, `filters.go`. Rules, orchestration, hot-path execution.
3. **Background Processes** — `*_worker.go`, `outbox_*.go`. Polling loops, replication, reconciliation.
4. **Persistence** — `postgres_store.go`, `clickhouse_store.go`, `redis_*.go`, `quota_repo.go`. sqlc, Redis, CH — co-located, not in a `repo/` package.
5. **Integration** — `provider_*.go`, `*_client.go`. Stripe, billing gRPC, notifier.
6. **In-Memory State** — `registry.go`, `settings.go`. Catalog replicas, config snapshots.

Transport and core logic reside in the same `package` and call each other directly. Separate code into files when size increases or responsibility shifts (R2), not into sub-packages.

Imports between services must be rare and explicit (e.g., calling ads registry helpers from management). Prefer interfaces in `internal/domain` for contracts shared by multiple binaries.

### Structs and Tags

Each type has **one primary role**. Tags declare this role. Do not attach `json` tags to a struct that is also used as a sqlc row type or a registry representation on the hot path.

1. **Hot-Path Model** — `internal/domain`. No tags allowed (`json`, `db` tags are forbidden). Names: `Campaign`, `Event`.
2. **SQL Row** — `internal/<svc>/db` (sqlc). `json` tags are generated from sqlc configuration. Names: `db.Campaign`.
3. **External Admin / REST Structs** — service root, `service_*.go`, `delivery_types.go`. Tags: `json:"snake_case"`. Names: `CampaignDTO`, `BrandCreativeDTO`.
4. **Replica / PubSub / File Payload** — service root, near the writing code. `json` tags for network transfer. Names: `campaignReplicaDTO`.
5. **gRPC / Ingestion Network Struct** — `internal/<svc>/pb` (generated protobuf). Names: `pb.Event`.
6. **Request Body / Command** — `handler*.go` (often unexported). `json` tags. Names: `createCampaignRequest`.

Conventions:
- `DTO` suffix: JSON types returned in the admin UI or external REST (R3).
- `Replica` suffix or `replica` prefix: Redis Pub/Sub, snapshot files, or JSON blobs between shards.
- Mapping functions reside next to the type: `toCampaignDTO`, `templateToDTO`, `campaignFraudConfigFromRow` — not in a separate `mappers/` package.
- Hot path builds `domain.Campaign` in `registry.go` from `db` rows. **FORBIDDEN:** processing catalog state through JSON on the ingestion path.

The `internal/domain` package is a shared vocabulary (status enums, `Event` pool, `Campaign` struct with precomputed Redis keys). It is **not** an entity layer: no persistence tags, no handlers, no `toDTO` helpers.

### Boundary Mapping

Mapping occurs **only** at I/O boundaries, **in a single step**:

```text
db.Campaign  --toCampaignDTO()-->  CampaignDTO  --json.Encode-->  Client

db.Campaign  --registry build-->  domain.Campaign   (in-memory; no JSON)

pb.Event     <--field copy / vtproto-->  Handler     (ingest; domain.Event pool)
```

1. **Admin Lists** — `cold.MapSlice(rows, toDTO)` or `cold.PaginatedList` — no intermediate entity slices.
2. **Admin Mutation / Retrieval** — the handler decodes the tagged request struct; the service accepts `uuid.UUID`, primitives, or `db` parameters.
3. **Outbox** — `json.Marshal` for a small specialized struct in `outbox_handlers.go`.
4. **gRPC** — `cold.MapSlice` to `pb.*` or manual copying; no third internal model.
5. **Hot-Path** — `requests_parse.go` parses into `domain.Event`; the registry returns pointers to `*domain.Campaign`.

**FORBIDDEN:** introducing `Entity`, `Model`, or `View` types duplicating the same table. **ALLOWED:** converting `pgtype` / `uuid` inside `to*DTO` or database write methods.

### `pkg/` vs `internal/`

1. **`pkg/cold`** — `MapSlice`, `PaginatedList`, `MarshalJSON`, pgx/uuid helpers — cold-path only (see R8.6).
2. **`pkg/httpcall`** — HTMX / JSON error wrappers.
3. **`pkg/logger`** — structured logging.
4. **`pkg/broker`** — optional mmap log broker (standalone).

Packages under `pkg/**` **MUST NOT** import `internal/<service>`. Packages under `internal/**` **MAY** import `pkg/**`.

---

## Requirements

### R1. Flat Service Packages

Each deployable domain area **MUST** reside as a **single flat Go package** under `internal/<name>/`.

1. Package name matches the directory: `package ads`, `package payment`.
2. All `.go` files reside in the directory root, in a single `package`.
3. Reference directory structures: `internal/ingestion/`, `internal/payment/`, `internal/management/`, `internal/auth/`.

**Allowed Subdirectories** (only generated code or schemas — no business logic, no additional domain packages):
1. `db/` — sqlc output (`package db`).
2. `queries/` — sqlc input (`.sql`).
3. `migrations/` — goose SQL migrations.
4. `pb/` — protobuf / vtproto generated code (`package pb`).

Non-Go resources bound to the service (e.g., `unified-filter.lua` in ads) **MUST** remain in the service root.

**FORBIDDEN:** nested domain packages under service roots such as `internal/ingestion/filter/`, `internal/rtb/auction/`, `internal/management/fraud/`. `internal/management/` is **one flat package** — navigate by filename prefix (`blacklist_*`, `service_fraud.go`), not subfolders. Only `pb/` (generated) is allowed under `management/`.

### R1b. Admin API (`internal/adminapi`)

`internal/adminapi/` is a **single flat package** for cold-path JSON `/api/v1`.

1. **Tag = first `_` segment** — `billing_handlers.go`, `ops_recon.go`, `selfserve_types.go`.
2. **Handler types** — `BillingHTTPHandlers`, `OpsHTTPHandlers`, … (unique per tag).
3. **Single register** — `register.go` mounts all handlers; `cmd/management` wires deps.
4. **No management import** — `adminapi` **MUST NOT** import `internal/management`.
5. **No subfolders** — `internal/adminapi/billing/` forbidden; domain = filename tag, not a nested package.
6. **R1 generated subdirs** — `db/`, `queries/`, `migrations/`, `pb/` allowed per R1 when this package owns SQL or protos. Today `adminapi` has none: reads go through `internal/billing/db`, `internal/billing/pb`, `internal/ingestion/sqlc`; subscription DDL stays in `internal/management/queries/` + migrations.
7. **Hot path unchanged** — `internal/ingestion/`, `internal/rtb/` remain flat per R1.

**Anti-patterns (reject in review):**
- New `/api/v1` handlers in `internal/management/handler_*.go`.
- Domain subpackages under `adminapi/` or `management/`.
- Names like `api`, `http`, `common`, or `utils` as filename tags.

### R1c. `internal/management` — flat package, tag navigation

`cmd/management` = **one** Go package (`package management`). No theme subfolders (`management/fraud/` forbidden).

**Grouping key for humans/IDE:** first `_` segment of the filename (or second for `service_<theme>_*.go` / `handler_<theme>_*.go`):

| File | Tag |
| :--- | :--- |
| `blacklist_janitor.go` | `blacklist` |
| `service_fraud.go` | `fraud` |
| `outbox_worker.go` | `outbox` |

Cross-cutting: `service.go`, `handler.go`, `workers.go`, `middleware.go`, `ops.go`, `errors.go`.

**Extract as sibling** only when shared across binaries (`internal/licensing/`) or separate HTTP surface (`internal/adminapi/`). ClickHouse connect + query helpers stay in `internal/database/` — no extra `chquery/` or `clickhouse/` subpackages for connection code.

Legacy `handler_*` / `service_*` pairs are **deprecated** — colocate on touch (`fraud_config.go`), do not extract subpackages.

### R2. File Naming

**Hot path and legacy cold path** — snake_case with a **domain prefix** before the first underscore:

```text
<domain>_<rest>.go
```

**New cold-path code in `adminapi`** — same tag convention as R1b (`billing_handlers.go`, `reports_metrics.go`). Prefer colocating HTTP + logic in one file when < ~500 LOC.

```text
# preferred (adminapi)
billing_handlers.go   reports_metrics.go   licensing_handlers.go

# legacy (management — colocate on touch)
handler_campaigns.go  service_campaigns.go
```

**Prefix / Template → Role (legacy + hot path):**
1. `service.go` — constructor, main struct, lifecycle (`NewService`, `Close`).
2. `service_<domain>.go` — business logic by domain area (`service_campaigns.go`).
3. `handler.go` — main transport entry point (`NewHandler`, `RegisterRoutes`).
4. `handler_<area>.go` — route group (`handler_billing.go`).
5. `http_<area>.go` — HTTP servers / middleware (`http_webhook.go`).
6. `<worker>_worker.go` — background polling loops (`outbox_worker.go`).
7. `outbox_*.go` — outbox handlers (`outbox_handlers.go`).
8. `provider_*.go` — external integrations (`provider_stripe.go`).
9. `errors.go`, `permissions.go` — small common helpers.
10. `*_test.go` — tests adjacent to code (`handler_campaigns_test.go`).
11. `*_bench_test.go` — benchmarks (`handler_proto_bench_test.go`).
12. `fault_*_test.go`, `*_chaos_test.go` — fault tolerance and chaos test suites (`fault_injection_test.go`).

Hot-path ads files follow the same pattern: `track_core.go`, `broker_consumer.go`, `sharding.go`.

**Rules:**
1. Split a file if it exceeds ~500 lines or mixes transport with business logic.
2. The prefix indicates the topic, not an architectural layer (`service_campaigns`, not `repository_campaigns`).
3. Tests must share the prefix of the code they test.

### R3. Types and Symbols

1. **Main Service Struct** — `Service` in the `service.go` file (e.g., `payment.Service`).
2. **Handler** — `Handler` or `<Area>Handler` (e.g., `WebhookHandler`).
3. **Admin/API Output Type** — `DTO` suffix, `json` tags (e.g., `CampaignDTO`).
4. **Redis/File/PubSub Payload** — `Replica` suffix or `replica` prefix (e.g., `campaignReplicaDTO`).
5. **Shared Hot-Path Model** — `internal/domain`, no struct tags (e.g., `domain.Campaign`).
6. **SQL Row** — `internal/<svc>/db`, generated by sqlc (e.g., `db.Campaign`).
7. **Domain Errors** — `Err…` prefix in the `errors.go` file (e.g., `ErrCustomerNotFound`). Hot-path reject: sentinel + `filterRejectKind` / `NoBidReason` (R8).
8. **Constructors** — `New<Type>` (e.g., `NewService`).
9. **DB Row to DTO Mapper** — `to<DTO>`, `<noun>FromRow` next to the type (e.g., `toCampaignDTO`).
10. **Unexported Helpers** — lowerCamelCase, without repeating package/type names (e.g., `finalizeDrainingCampaign`).

See full rules in **Structs and Tags** and **Boundary Mapping** sections.

### R4. Binaries under `cmd/`

```text
cmd/<binary>/main.go
```

Binary name **MUST** match the process name (`tracker`, `management`, `payment`, `auth`). `main` only binds configuration, pools, and `internal/<service>` constructors — **FORBIDDEN:** placing business logic in `main`.

### R5. Imports

Import structure guidelines:
- Import the main flat service package for binary entry points (`internal/payment`).
- Import generated sqlc database models explicitly (`internal/payment/db` aliased as `paymentdb`).
- Import generated Protobuf/gRPC message packages (`internal/payment/pb` aliased as `paymentpb`).

1. Import the flat service package for the API: `payment.NewService`.
2. Import `db/` and `pb/` only where sqlc or gRPC types are required.
3. **FORBIDDEN:** importing non-existent sub-packages (`management/service`, `ads/filter`).

### R6. Domain Terminology

Consistently use abbreviations accepted in the project:
1. **`evt`** — `*domain.Event` on the hot path in ads.
2. **`rdb`** — Redis client (`go-redis`).
3. **`camp` / `campInfo`** — campaign metadata in filter code.
4. **`fraudAcc`** — fraud signal accumulator (not `acc`).
5. **`rateLimitKeyBuf`, `dupKeyBuf`, …** — pooled `bufWrapper` for Redis keys.

Other local variables **MUST** use informative names, not arbitrary abbreviations.

### R7. Blank Identifier (`_`)

**ALLOWED** to ignore:
1. BCE hints: `_ = slice[len(slice)-1]`
2. `defer tx.Rollback(ctx)` in cold-path transactions.
3. Benchmark bodies that intentionally discard results.
4. Return values of the `Write` method of the Hash interface.

**MUST** rewrite APIs that always succeed (e.g., `NewFastUUID()`) so they do not return a dummy `error`.

**MUST** explicitly handle on the cold path: `json.Marshal`/`Unmarshal`, `uuid.Parse`, decoding HTTP request bodies, outbox payload errors — return or log; do not discard.

**I/O on the hot path (best-effort):** see R8.3 pt. 6 — increment a counter without blocking the hot loop; do not use `_ =` blindly.

### R8. Error Handling

These rules supplement R7 (blank identifier `_`). On the hot path, performance constraints in `.cursorrules` take precedence over idiomatic Go.

#### R8.1. Hot-Path and Cold-Path Boundaries

| Zone | Packages / Code | Transport |
|------|----------------|-----------|
| **Hot-path** | `internal/ingestion` ingest, `FilterEngine.Check`, `processTrack`, `internal/rtb` auction, gnet scanner | `/track`, OpenRTB bid |
| **Cold-path** | `internal/management`, `internal/payment`, `internal/auth`, `internal/billing`, workers, outbox | Admin HTTP, gRPC, webhooks |

Hot-path: rejecting a request is an expected outcome (budget, geo, no-bid), not a control flow exception. Cold-path: an error is a signal for the caller, retry, or HTTP code.

#### R8.2. Cold-Path (Idiomatic Go)

1. **Sentinels** — declare in `errors.go` or near the service: `var ErrCampaignNotFound = errors.New("...")`. Compare using `errors.Is` / `errors.As`, not `err.Error() ==` (except for stable validation strings in `mapServiceError`).
2. **Wrapping** — when crossing boundaries (sqlc -> service -> handler): `fmt.Errorf("list campaigns: %w", err)`. Context on the outside, `%w` preserves the chain for `errors.Is`.
3. **`pgx.ErrNoRows`** — only for "resource not found". Other Postgres errors **MUST NOT** be masked as `ErrNotFound` / 404. Example: `GetRtbDeal` returns `ErrRtbDealNotFound` only on `errors.Is(err, pgx.ErrNoRows)`.
4. **HTTP Handlers** — single mapping point:
   - `mapServiceError` + `writeServiceError` — domain and infra errors; 5xx log via `slog.Error`, return sanitized `{"code","message"}` body to client.
   - Validation (`required`, `must be`, `ErrInvalid*`) — 400 with the error text.
   - Domain mappers (`writeRtbDealError`) — explicit `errors.Is` for known `Err*`, default -> `writeServiceError`.
5. **Outbox / Workers** — handler returns `error` -> row remains `PENDING`/`PROCESSING` and is retried. Permanent failure -> mark + ops alert. Log: `slog` + structural attributes (`campaign_id`, `event_type`).
6. **Serialization** — `json.Marshal`, `uuid.Parse`, body decode: explicit check and return (R7). **FORBIDDEN:** `payload, _ := json.Marshal(...)` in production code.
7. **gRPC** — map to `codes.NotFound`, `InvalidArgument`, `Internal` at the handler boundary; do not pass raw `err.Error()` to the client.
8. **FORBIDDEN:** `log.Fatal` and `panic` in request processing paths (acceptable only in `main` on fatal misconfiguration).

- **HTTP Error Boundary**: Management handlers write service errors using `mapServiceError` and `writeServiceError`. 5xx errors log structural attributes via `slog.Error`, while client responses receive a sanitized JSON error payload.
- **DB Error Boundary**: Service layer checks `errors.Is(err, pgx.ErrNoRows)` explicitly to translate database missing-row errors to domain sentinels (e.g., `ErrRtbDealNotFound`) while returning unmapped database errors directly.

#### R8.3. Hot-Path (Allocation-Free)

1. **Sentinels Without Formatting** — `var ErrBudgetExhausted = errors.New("budget exhausted")` in `filters.go`. **FORBIDDEN:** `fmt.Errorf` / `errors.New(fmt.Sprintf(...))` on the parse -> filter -> respond path.
2. **Typed Codes Instead of `error` in Core** — RTB: `(AuctionResult, NoBidReason)`; track: `trackOutcome{Status, RejectKind}`. `NoBidReason` and `filterRejectKind` are `uint8` enums with pre-bound `.String()` methods used only for metrics.
3. **Classification at the Boundary** — a single `classifyFilterErr(err)` call before responding; internally uses `errors.Is` for sentinels, no reflection. Unclassified -> `trackStatusInternalError` + `filterEngineFailures.Inc()`, no per-request `slog` (lossy by design).
4. **Allocation-Free Client Response** — `filterRejectSpecs` table: pre-built `[]byte` gnet responses and HTTP statuses. **FORBIDDEN:** `json.Marshal` / `fmt.Sprintf` for each reject.
5. **Redis / Lua** — deadline from `FilterDeadlineMono` -> client timeout. `context.DeadlineExceeded` -> `filterRejectTimeout`; `database.ErrRedisCircuitOpen` / network -> `filterRejectInfra`. Geo MaxMind -> fail-open (separate `ErrGeoBlocked` sentinel only on explicit match).
6. **Best-Effort Side Effects** — `FraudStreamWriter`, `LatencyRing`: drop on overflow, increment drop counter; rare sampled logs (R7). **FORBIDDEN:** blocking the hot loop waiting for I/O.
7. **FORBIDDEN on the hot path:** `defer` in loops, `interface{}` error boxing in inner loops, `log.Fatal`, dynamic error string construction for metrics (use pre-bound label values).
8. **Type Assertions** — only safe snapshots (`campaignMapSnapshot()` — R8.7 pt. 2). **FORBIDDEN:** `m, _ := x.Load().(map[...])`.

- **Hot-Path Classification**: Ingest and track handlers pass filter errors to `classifyFilterErr(err)` to convert sentinels directly into typed `trackOutcome` statuses without HTTP error wrapping. Unclassified errors increment `filterEngineFailures` counters and return `trackStatusInternalError`.
- **RTB Auction Rejects**: OpenRTB auctions return typed `NoBidReason` enums without creating error interfaces, bridging reasons directly to `filterRejectKind` metrics.

#### R8.4. Error Handling Summary

| Scenario | Hot-path | Cold-path |
|----------|----------|-----------|
| Expected reject (budget, geo, no-bid) | `Err*` or `NoBidReason` -> `filterRejectKind` | `Err*` -> 4xx via mapper |
| Validation | Parse fail -> `trackStatusRejected` / 400 body | 400 + message |
| Infra (Redis down, timeout) | `filterRejectInfra` / `filterRejectTimeout` + counter | `%w` return, worker retry, 503/500 |
| Unknown error | Internal status + metric, no log storm | `slog.Error` + sanitized 500 |
| Not found | `filterRejectCampaignNotFound` | `errors.Is(pgx.ErrNoRows)` -> 404 |

#### R8.5. Antipatterns
1. Masking all database errors as `not found` (diagnostic loss, incorrect HTTP code).
2. `fmt.Errorf` on each filter reject in `/track`.
3. Returning `error` from `RunAuction` instead of `NoBidReason`.
4. Logging every 204/reject on the hot path (cardinality and alloc pressure).
5. Passing raw `pgx`/`redis` error text to JSON admin API.

#### R8.6. Cold-Path: Additional Idiomatics

1. **`pkg/cold`** — single point for `json.Marshal`/`Unmarshal`, `uuid.Parse`, pagination (`MapSlice`, `PaginatedList`). **FORBIDDEN:** duplicating wrappers in each service.
2. **Typed Validation Errors** — for form fields and queries, use unexported types (`invalidQueryError`, `validationError`) and `errors.As` in the mapper. **FORBIDDEN:** relying on `strings.Contains(err.Error(), "required")` for new code if a stable type exists.
3. **`errors.Join`** — in batch/outbox/worker where multiple independent failures occur in one iteration, combine them using `errors.Join` instead of string concatenation or returning only the first error.
4. **Single-Handling Rule** — log or return errors at boundaries, never both at the same level. `Service` returns `error`; `handler` calls `writeServiceError` and logs 5xx exactly once.
5. **`atomic.Value` on Cold Reload** — even outside the hot path, snapshot via typed helper (see R8.7 pt. 2), not `m, _ := x.Load().(map[...])`.
6. **Silent `json.Unmarshal`** — **FORBIDDEN:** `if json.Unmarshal(...) != nil { return }` without `slog` and/or counter. Corrupt replica -> warn + drop, not panic.
7. **Infra Errors** — `database.IsNetworkOrSystemError` and `errors.As` for `*net.OpError` before falling back to `strings.Contains` checks on text.

Validation and worker batch guidelines:
- **Validation**: Handlers check for typed validation errors using `errors.As(err, &ve)` to return a structured HTTP 400 Bad Request message without raw string checks.
- **Worker Batching**: Worker flush loops combine batch execution failures and status update errors via `errors.Join(err, markPending(rows))` to preserve error context.

#### R8.7. Hot-Path: Performance

R8.3 specifies allocation-free reject and parse. Below are additional rules for latency, cardinality, and algorithms. In case of conflict with R8.3, `.cursorrules` takes precedence.

1. **Metrics Without Dynamic Labels** — **FORBIDDEN** on the parse -> filter -> respond path:
   - `prometheus.*.WithLabelValues(evt.CampaignID.String(), ...)`
   - `WithLabelValues(err.Error(), ...)`
   Use pre-bound counters/histograms (`filterRejectSpecs`, `preboundTrackMetrics`) or a fixed label set (`shard`, `reason`). Per-entity cardinality -> cold-path or sampled logs only.
2. **Typed Snapshot for `atomic.Value`** — Stores write an immutable snapshot wrapper struct (e.g., containing `byID map[uuid.UUID]campaignInfo`). Readers access the snapshot via a dedicated helper function that performs a safe type assertion to the concrete pointer, returning a zero-value fallback struct on failure. Direct untyped `Load().(map[...])` assertions are strictly forbidden.

   **FORBIDDEN:** `m, _ := r.data.Load().(map[uuid.UUID]...)`.

3. **Redis on Hot-Path** — one `EVALSHA` per event (unified filter, IP rate limit Lua). **FORBIDDEN:** `Pipeline()` + multiple `New*Cmd` per filter check if an equivalent Lua script exists.
4. **Protobuf Wire Format** — for metadata on `/track`, prefer `extra_bytes` (a single `append[:0]`). `extra_keys` / `extra_values` are allowed but require patching (R11) and the `TestAdEvent_UnmarshalVT_ExtraRepeated_ZeroAlloc` test after `make proto`.
5. **RTB Auction** — cold-path `UpdateCampaigns` sorts the bucket by score; hot-path `rankCandidates` exits early after winner + second when scores decrease. Do not rely on O(n) scans on dense shards without indexes.
6. **Deadline Without Allocating Context** — filter deadline via `evt.FilterDeadlineMono`, not `context.WithTimeout` per `/track`. `FilterEngine.Check` may receive `context.Background()`; the Redis client timeout is derived from the monotonic clock.
7. **APIs Without Dummy Errors** — functions that always succeed (`NewFastUUID()`) do not return an `error` (R7).
8. **Escape Analysis** — for affected hot-path files: `go test -gcflags='-m' ./internal/ingestion/... 2>&1 | rg 'escapes'`. New escapes on parse/filter/respond block PR merge (R10).
9. **Benchmark Baseline** — baseline figures in `docs/hot_path_baseline.md` and raw reports in `docs/hot_path_baseline_*_raw.txt`. Regressions are caught by the CI perf gate (R11).

| Antipattern | Why | Alternative |
|-------------|-----|-------------|
| `uuid.String()` in metrics | alloc + cardinality | pre-bound label / shard counter |
| `Load().(map)` | panic risk, no contract | typed snapshot |
| `fmt.Errorf` on reject | alloc on hot path | `Err*` + `classifyFilterErr` |
| Pipeline INCR+PEXPIRE | 3-5 allocs/cmd | `ip-rate-limit.lua` |
| repeated protobuf bytes | 4 allocs/field | `extra_bytes` or `appendReuseBytes` patch |

### R9. Comments

Comments explain **why** a decision was made or **what will break if changed** - they must not describe names, control flow, or personal opinions.

**Rules:**
1. English only.
2. Only ASCII characters allowed (`0x20`-`0x7E`).
3. Forbidden: emojis, Unicode section signs (`§`), Unicode dashes (`—`, `–`), curly quotes, decorative symbols, arrows (`→`, `←`).
4. Punctuation: hyphen-minus `-` for parenthetical constructs and numeric ranges; end multi-sentence blocks with a period `.`.
5. Tone: dry, factual, third person or imperative where appropriate; no self-promotion, hype, or subjective assessments.
6. Grammar: use complete sentences with explicit subject and verb. Avoid sentence fragments, telegraphic style, and stacked semicolon clauses where separate sentences are clearer.
7. Document cross-references: write `docs/FOO.md section 4.3`, not `FOO.md §4.3`.

**Avoid** vague adjectives: `simple`, `elegant`, `clean`, `obviously`, `just`, `simply`, `nice`, `minimal`, `trivial`.

**Comment in the following cases:**
1. Non-obvious exported contract (e.g., `NewService` starts an outbox worker; the caller must call `Close`).
2. Business invariant (e.g., cancellation fee applies to remaining budget, not spent budget).
3. Performance constraints (e.g., one EVALSHA call per event; additional network requests violate SLA).
4. Correctness of failure handling (e.g., returning an outbox row to `PENDING` status on Redis failure).
5. Specific external characteristics (e.g., shard 0 only: trackers subscribe to `campaigns:update`).
6. Trust boundary (e.g., processing geolocation in fail-open mode when MaxMind is unavailable).
7. BCE / hot loop (e.g., `// BCE hint: len check hoists bounds for loop below`).
8. Non-obvious test purpose (e.g., fcap key at the brand level, not for a specific campaign).

**Do not comment:** duplicate names, step-by-step execution descriptions, trivial getters, duplicate field values.

**Default:** unexported symbols do not require godoc if none of the rules above apply. Exported API: godoc for types and entry points used by the caller.

**Placement:**
1. Package comment (optional): a single block in `service.go` or `handler.go` if the exported API requires a service-level overview. In `tests/`, one package comment per test package is enough; additional files may use a one-line file header describing that file's scope.
2. Struct fields: only when the logic is not obvious from the name.
3. Inside function body: almost never - exceptions: BCE, `//go:` directives, `//nolint:` with a reason, links to workarounds.

#### R9.2. Test Comments

Tests under `tests/` follow the same ASCII and tone rules as production code (R9).

**Test function godoc** must state:
1. The scenario under test (setup, action, or fault injected).
2. The expected invariant or observable outcome (HTTP status, row count, budget value, outbox status).
3. An external doc reference when the test implements a named scenario (e.g., `CHAOS.md section 6 scenario A`).

**File headers** in `tests/` are allowed when a package spans multiple files and each file covers a distinct area (e.g., `flow_test.go`, `multishard_test.go`).

**Helper functions** shared within a test file should have a one-line godoc when the name alone does not convey return values or side effects.

- **Good Comment Patterns**: Explains why a test exists, scenario ID, or non-obvious outcome (e.g., `TestE2E_Idempotency` referencing scenario 4.3 with exact expected status 202 and zero double-debit).
- **Bad Comment Patterns**: Fragments, subjective words, non-ASCII emojis, or telegraphic text (e.g., `verifies §4.3 - no double debit 🎯`).

#### R9.1. Automated Comment Verification

R9 requirements **MUST** be verified automatically in pre-commit or CI (script `scripts/ci/check_comments.sh` or `lefthook` equivalent).

**Mandatory Checks (fail on match in diff / in `internal/`, `pkg/`, `cmd/`, `tests/`):**
1. **Non-ASCII** - characters outside `0x20`-`0x7E` in comments and godoc (except generated code in `*/pb/`, `*/db/`).
2. **Emoji** - Unicode emojis in comments.
3. **Forbidden Words** (case-insensitive in comments): `simple`, `elegant`, `clean`, `obviously`, `just`, `simply`, `nice`, `obvious`, `trivial`, `minimal`.
4. **Unicode Dashes and Section Signs** - `—`, `–`, `§` in comments (use `-` and `section N`).

```bash
# scripts/ci/check_comments.sh (sketch)
git diff --cached -U0 -- '*.go' | rg '^\+\s*//' | rg '[^\x00-\x7F]' && exit 1
rg -n '//.*(simple|elegant|obviously)\b' internal/ pkg/ cmd/ tests/ --glob '!**/pb/**' && exit 1
```

**Exceptions:** generated files are not scanned. Test files follow the same rules as production code (R9.2).

**PR Checklist:** new comments in the diff pass `check_comments` locally or in CI.

### R10. PR Verification

1. **Hot-path** — `go test -benchmem` for affected benchmarks; the `0 allocs/op` metric must remain unchanged (R8.3, R8.7).
2. **Cold-path** — `go test ./... -short`.
3. **All** — no new ignored marshaling/parsing errors in production code (R7, R8.6).
4. **Comments** — checklist R9 and R9.1 automated checks.
5. **Lint** — `make lint` (golangci-lint + staticcheck SA9003, SA4017; errcheck on non-test — R11).
6. **Perf Gate** — on changes in `internal/ingestion`, `internal/rtb`, `pkg/logger`, `api/`: workflow `perf-gate.yaml` (smoke zero-alloc; strict benchstat when `PERF_RUNNER_LABEL` is set).

### R11. Tooling and Codegen

#### R11.1. Protobuf / vtproto

1. **Single Generation Point** — `make proto` -> `scripts/codegen/gen.sh --proto` -> `api/buf.gen.yaml` -> staging `api/gen/` -> `safe_sync_proto_gen` in `internal/*/pb/`.
2. **Post-Gen Hot-Path Patch** — after syncing, `go run ./scripts/codegen/patch_vtproto_hotpath` is mandatory:
   - replaces `make+copy` with `appendReuseBytes` for `EventMetadata.ExtraKeys` / `ExtraValues`;
   - the helper lives in `internal/ingestion/pb/unmarshal_helpers.go` (not generated).
3. **Patch Extension** — when adding repeated `bytes` on hot ingest:
   - add the pattern to `scripts/codegen/patch_vtproto_hotpath/main.go`;
   - add a reuse helper to `unmarshal_helpers.go`;
   - add `TestAdEvent_UnmarshalVT_*_ZeroAlloc` or E2E bench with `ReportAllocs`.
4. **Idempotency** — running `make proto` repeatedly does not break the tree; patch is a no-op if `appendReuseBytes` is already applied.
5. **Patch Failure** — exit code 1 with the text `pattern missing` indicates a change in vtproto output; update patterns, do not edit `events_vtproto.pb.go` manually.

```text
make proto
  -> buf generate (api/gen)
  -> rsync internal/*/pb
  -> patch_vtproto_hotpath
  -> go test ./internal/ingestion/... -run UnmarshalVT
```

#### R11.2. Staticcheck and errcheck (CI / `make lint`)

The following **MUST** be enabled in `.golangci.yaml` (or equivalent via `staticcheck`):

| Rule | Purpose |
|------|---------|
| **SA9003** | Empty branch after `if` / `else` - dead code and hidden bugs |
| **SA4017** | Incorrect use of `crypto/rand` / deprecated RNG |

**errcheck on non-test:**
1. Enable `errcheck` for production code (`internal/`, `pkg/`, `cmd/`).
2. **Exceptions** are only explicit in `.golangci.yaml`:
   - `*_test.go`;
   - generated code `*/pb/`, `*/db/`;
   - intentional `Close`/`Write` on the shutdown path (with `//nolint:errcheck` and a reason).
3. **FORBIDDEN:** globally disabling errcheck for all of `internal/management` or `internal/ingestion`.

#### R11.3. Benchmark Baseline in CI

1. **Documentation** — `docs/hot_path_baseline.md` (summary) + `docs/hot_path_baseline_ads_raw.txt`, `docs/hot_path_baseline_rtb_raw.txt` (raw, 5 runs).
2. **Local Gate** — `bash scripts/perf-gate/perf_gate_run.sh` or `task perf-gate`:
   - smoke: `scripts/perf-gate/perf_gate.go` verifies 0 allocs on gated benches;
   - strict (self-hosted): benchstat vs baseline worktree `main`.
3. **CI Workflow** — `.github/workflows/perf-gate.yaml`:
   - trigger on push to `main` and path-filter (`internal/ingestion/**`, `internal/rtb/**`, `api/**`, `scripts/perf-gate/**`);
   - artifacts: `pr_bench.txt`, `baseline_bench.txt`, `gate_report.txt`.
4. **Gated Benches** — list in `scripts/perf-gate/perf_gate_bench.sh` (E2E accept/reject/infra, filter micro, RTB auction). A new hot-path bench **MUST** be added to the gate when a critical path is introduced.
5. **Baseline Update** — upon intentional performance improvements, merge the PR with updated `docs/hot_path_baseline*.md/txt` in the same commit.

#### R11.4. Other Codegen Hooks

New post-gen patches follow the same contract as `patch_vtproto_hotpath`:
1. Standalone command in `scripts/codegen/`.
2. Invocation from `gen.sh` after the corresponding generate.
3. Fail-fast on generator template changes.
4. Test or bench securing the invariant.

---

## Instructions

### New Service or Major Refactoring

1. Create `internal/<name>/` with a single `package <name>`.
2. Add subdirectories `db/`, `queries/`, `migrations/`, `pb/` if needed (and nothing else).
3. Add `service.go` and `handler.go` files (or their gRPC equivalents).
4. Split logic into files like `service_<domain>.go` / `handler_<area>.go` instead of creating sub-packages.
5. Create `cmd/<binary>/main.go` to handle dependency binding only.
6. Extract shared hot-path types or store interfaces to `internal/domain` only when required by multiple binaries.
7. Add `*DTO` types with `json` tags in `service_*.go` or `delivery_types.go`; map from `db.*` in a single step.
8. Verify import cycles: flat structure should eliminate them.

### Adding a Domain Type

1. Hot-path or contract for multiple binaries: `internal/domain`, no tags, precomputed fields allowed.
2. Admin JSON Response: `FooDTO` in the service package with `json:"snake_case"` tags.
3. Redis/PubSub/File Cache: `fooReplicaDTO` with `json` tags next to serialization code.
4. Persistence: extend schema/sqlc queries; use `db.Foo` — do not duplicate it as a manual database row struct.
5. **FORBIDDEN:** adding an intermediate `FooEntity` model between `db.Foo` and `FooDTO`.

### Adding a New Source Code File

1. Select a prefix from R2 matching the file's purpose.
2. Maintain one primary area of responsibility per file.
3. Add a `*_test.go` file with the matching prefix; fault tolerance tests must use the `fault_*` prefix or `*_chaos_test.go`.

### Adding an Error or Reject Path

**Cold-path:**
1. Declare `var ErrFoo = errors.New("stable message")` in `errors.go` or `service_<domain>.go`.
2. Service returns `ErrFoo` or `fmt.Errorf("...: %w", err)` for infra.
3. Handler: `errors.Is(err, ErrFoo)` -> desired HTTP code; otherwise `writeServiceError`.
4. For outbox: handler returns `error`; do not swallow Redis/PG failures.

**Hot-path:**
1. Add a sentinel to `filters.go` (or an enum in `internal/rtb` for auction).
2. Filter/auction returns the sentinel or `NoBidReason`, no string formatting.
3. Register in `classifyFilterErr` / `noBidToRejectKind` -> `filterRejectKind`.
4. Add a row to `filterRejectSpecs` (status, pre-built body, metric label).
5. Benchmark: 0 allocs/op on the affected path.

### Adding a Hot-Path Optimization

1. Measure baseline: `go test -benchmem -count=5` (see `docs/hot_path_baseline.md`).
2. Verify the path adheres to R8.3 (sentinels, pre-built responses) and R8.7 (metrics, snapshots, Lua).
3. Add or update `*_bench_test.go`; for E2E paths, add a bench in `perf_gate_bench.sh`.
4. On protobuf ingest changes, update `patch_vtproto_hotpath` if needed (R11.1).
5. Run `bash scripts/perf-gate/perf_gate_run.sh` locally when modifying `internal/ingestion` / `internal/rtb`.

### Writing Comments

- **Good Godoc Practices**: State specific operational constraints, cross-shard behavior, or non-obvious worker scheduling rules (e.g., specifying that a pub/sub update runs on shard 0 only, or that an outbox worker interval decreases under queue backlog).
- **Bad Godoc Practices**: Stating trivial getter operations, repeating function signatures, or using subjective words like "simple", "fast path", or non-ASCII characters.

---

## Checklist

### New Service or Refactoring
- [ ] One `package` in the root of `internal/<name>/` (R1)
- [ ] Only allowed subdirectories (R1)
- [ ] Files named using snake_case with domain prefixes (R2)
- [ ] Presence of `service.go` + `handler.go` (R2)
- [ ] No nested domain packages, entity layers, or triple copies of models (Tactical Architecture)
- [ ] DTOs have `json` tags; `domain` types have no tags; mapping is one-step at the boundary (Tactical Architecture, R3)
- [ ] No artificial sub-package separation / import cycles (R1, R5)

### Pull Request
- [ ] Hot-path: `-benchmem`, 0 allocs/op (R10, R8.7)
- [ ] Cold-path: `go test ./... -short` (R10)
- [ ] No ignored marshaling/parsing errors (R7, R8.6, R10)
- [ ] Errors: cold-path via `errors.Is` / `%w` / typed validation; hot-path without `fmt.Errorf` on reject (R8)
- [ ] HTTP: validation -> 400; infra -> log + sanitized 5xx; `pgx.ErrNoRows` not mixed with other DB errors (R8)
- [ ] Hot-path metrics: no `WithLabelValues` with `uuid.String()` / `err.Error()` (R8.7)
- [ ] `atomic.Value`: typed snapshot, not bare `Load().(map)` (R8.7)
- [ ] Comments: English only, ASCII only, complete sentences, no redundant godoc (R9, R9.2)
- [ ] Comments: `check_comments` / R9.1 (local or CI)
- [ ] New exported API documents contract or failure mode (R9)
- [ ] `make lint` passes; SA9003, SA4017, errcheck non-test (R11)
- [ ] On hot-path / proto change: `make proto`, perf gate green (R11)
- [ ] On perf baseline change: `docs/hot_path_baseline.md` updated (R11.3)

---
