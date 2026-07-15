# Go hot path: runtime, allocations, and ingestion internals

Tracker ingestion must stay within SLA: p95 &lt; 50 ms, p99 &lt; 80 ms, 100 ms hard ceiling on gnet (measured without edge). This document covers `gnet`, worker pools, zero-allocation policy, compiler tuning, and tracker-side mechanics. Control-plane UDP recovery is in [EDGE.md](./EDGE.md) Part III §1; edge ingress is in [EDGE.md](./EDGE.md) Part I.

**See also:** [ARCHITECTURE.md](./ARCHITECTURE.md) (topology), [DEVELOPMENT.md](./DEVELOPMENT.md) (perf gate, chaos pyramid).

---

## 1. gnet and PinnedWorkerPool

`net/http` spawns a goroutine per request. At high RPS that increases scheduler churn, stack growth, and GC pressure.

### gnet event loop

- Linux `epoll` multiplexing; fixed event-loop threads (typically 2 per CPU).
- DFA HTTP/1.1 scanner parses from the socket ring buffer without heap allocation.
- Per-connection `connContext` — no global `sync.Pool` on the request path.

### PinnedWorkerPool

After parse, work is pinned to a worker via campaign hash into an MPSC ring:

- Improves L1/L2 locality for campaign-scoped filter state.
- Queue indices are cache-line padded (64 B) to avoid false sharing (`worker_pool.go`).
- `gnet.WithLockOSThread(false)` — workers are not bound to OS threads; pinning is logical per queue.

---

## 2. Zero-allocation policy

Heap allocation on the hot path is prohibited. CI enforces `0 allocs/op` on gated benchmarks (`scripts/perf-gate/perf_gate_bench.sh`).

### Techniques

| Technique | Where | Notes |
| :--- | :--- | :--- |
| vtproto pools + `UnmarshalVT` | `internal/ingestion/pb/` | Reuse message structs; `bytes` fields slice socket buffers |
| Byte-slice query parse | `requests_parse.go` | No `json.Unmarshal` on `/track` |
| `unsafe.String` / `unsafe.Slice` | Parse helpers | `runtime.KeepAlive` at boundaries |
| Pre-bound Prometheus labels | `metrics_prebound.go` | No dynamic label maps per request |
| Fixed arrays / stack buffers | Fraud ring, latency ring | Lossy overflow by design |
| Lua `EVALSHA` single round trip | `unified_filter.go` | One Redis call per accepted event |
| Monotonic deadlines | `filter_context.go` | `runtime.nanotime` via linkname; immune to NTP jumps |

### Zero-copy strings

```go
func unsafeString(b []byte) string {
    if len(b) == 0 {
        return ""
    }
    return unsafe.String(&b[0], len(b))
}
```

Callers must not retain the string past the backing buffer lifetime.

### Local vs global pools

Connection-scoped context reuse avoids `sync.Pool` contention across CPUs on the ingest path.

---

## 3. Monotonic time and filter deadlines

Wall clock is not used for request budgets. On filter entry:

```go
evt.FilterDeadlineMono = monotonicNano() + timeout.Nanoseconds()
```

Redis and other clients shrink their timeouts to the remaining monotonic budget. TTC (time-to-click) uses monotonic deltas; UDP coarse wall time (`clock_udp.go`) adjusts display timestamps only, never TTC deadlines.

Chaos: `clock_drift_chaos_test.go` — +3600 s wall drift does not extend filter deadlines.

---

## 4. Compiler analysis (BCE, escape, inline)

### Bounds-check elimination

Hint max index at loop entry so the compiler elides per-index checks:

```go
if len(items) < 4 {
    return
}
_ = items[3]
```

Verify: `go build -gcflags="-d=ssa/prove/debug=1" ./internal/ingestion/...`

### Escape analysis

```bash
go build -gcflags="-m -m" ./internal/ingestion/... 2>&1 | grep "escapes to heap"
```

Nightly CI: `scripts/perf-gate/escape_nightly_job.sh`.

### Inlining

```bash
go build -gcflags="-m" ./internal/ingestion/... 2>&1 | grep "can inline"
```

Low-complexity hot-path helpers should stay inline-friendly (no closures, minimal branches).

---

## 5. Filter engine (Go layer)

`FilterEngine.Check` runs before tiered Lua ([EDGE.md](./EDGE.md) Part II §5):

1. Emergency breaker (`SettingsWatcher`)
2. Fraud (MaxMind, fail-open)
3. Geo (fail-open)
4. Schedule (registry snapshot)
5. ML boost via `GetFraudScoreBoosts()` in fraud accumulator — reads `ml:score:boost:{campaign_id}` from `SettingsWatcher` snapshot; 0 allocs, no `fraudscoring` import ([ARCHITECTURE.md](./ARCHITECTURE.md#fraud-scoring-cold-path))
6. `UnifiedFilter` — Lua tier B/C

Shared deadline propagates to Redis client timeout.

---

## 6. Ingestion components

### Budget cache warmer

`BudgetCacheWarmer`: pipelined `SET NX` for `budget:campaign:*` on registry sync. Avoids overwriting live decrements; PG/sync worker corrects drift.

### Brand creative routing

`BrandCreativeStore`: `atomic.Value` map; FNV-1a over `userID + brandID` for weighted segment.

### Telemetry (lossy)

| Component | Overflow |
| :--- | :--- |
| `LatencyRing` | Overwrite oldest |
| `FraudStreamWriter` (4096 slots) | Drop + metric |
| Audit log (sampled) | Drop |

### Health

Background 2 s Redis probe per shard; `/health` returns `DEGRADED` when any shard fails.

### Ingress quota gate

When `UDP_CONTROL_ENABLED=true`, `IngressQuotaCell` in gnet `React` runs before parse/filter. See [EDGE.md](./EDGE.md) Part III §1.

---

## 7. Benchmark reference

Median hot-path targets (amd64, CI perf gate):

| Benchmark | Target |
| :--- | :--- |
| `BenchmarkFilterFraudBoost` | ~90 ns, 0 B, 0 allocs/op |
| `GetShard` (StaticSlot) | ~5.6 ns, 0 allocs/op |
| Gated `BenchmarkAdsPacketHandlerProto` | 0 allocs/op (CI smoke) |

Run locally: `make test-alloc-gate`, `bash scripts/perf-gate/perf_gate_run.sh`.
