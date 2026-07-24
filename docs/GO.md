# Hot Path (Go)

Tracker ingestion: `internal/ingestion`, `internal/rtb`. Architecture: [ARCHITECTURE.md](./ARCHITECTURE.md). Persistence: [DATA.md](./DATA.md) Part I.

---

## SLA

| Metric | Target |
| :--- | :--- |
| Handler `ad_http_request_duration_seconds` | p95 < 50 ms, p99 < 80 ms, max 100 ms |
| Redis Lua (unified-filter / budget-fast) | p99 < 10 ms / shard |
| Geo filter (sampled) | p99 < 10 µs |
| `RunAuction` | p99 < 15 µs; scanned candidates p99 < 500 |
| Parse / filter / auction | 0 allocs/op (`make test-alloc-gate`) |

Production: `FILTER_TIMEOUT_MS` ≤ 100.

---

## gnet and PinnedWorkerPool

- Event loop: `epoll`; ~2 threads per CPU core.
- HTTP/1.1: table FSM (`http1_fsm.go`); incremental parse from ring buffer.
- `PinnedWorkerPool`: dispatch by `campaign_id` hash; 64-byte queue padding against false sharing.
- `LocalQuantaLedger` (M8): process-global per campaign hash; `TrySpendLocal` ~13 ns/op; refill on cold goroutines. [CAPABILITIES.md](./CAPABILITIES.md#m8--local-budget-quanta).

---

## Zero-allocation rules

Forbidden on request path: `defer` in loops, closures, `interface{}` boxing, `sync.Map`, `fmt.Sprintf`, string `+`, dynamic Prometheus labels, `context.WithValue`.

Allowed techniques:

| Technique | Use |
| :--- | :--- |
| vtproto pools | Event structs |
| Byte-slice parse | Query/body from socket buffer |
| `unsafe.String` | Zero-copy string view; lifetime ≤ gnet frame |
| Pre-bound metrics | Counter/histogram labels at init |
| Stack `[N]byte` + append | Key formatting |

---

## Monotonic deadlines

`FilterDeadlineMono = monotonicNano() + timeout` at request entry. No wall clock in filter loops.

---

## Compiler verification

```bash
go test -run='^$' -bench=. -benchmem ./internal/ingestion/...
make test-alloc-gate
go build -gcflags="-m" ./internal/ingestion/...  # escape analysis (sample)
go tool objdump -s 'package\.Func' ./tracker  # hot loop: no panicIndex, no morestack in inner loop
```

| Signal in asm | Meaning |
| :--- | :--- |
| `CALL runtime.panicIndex` | Missing BCE |
| `CALL runtime.morestack` | Deep stack / no inline |

---

## Data-oriented design

- Prefer SoA over AoS for candidate scans (RTB catalog).
- No pointer chains on hot path; materialize flat slices on cold rebuild.
- Presort buckets for predictable early-exit branches.
- Bitmasks instead of nested `if` where possible.

---

## Atomics and false sharing

- Pad contended atomics: `_ [56]byte` or `cpu.CacheLinePad` between fields.
- Load atomics once outside tight loops.
- Config snapshots: `atomic.Pointer` swap, not per-field stores.

---

## FilterEngine (Go layer)

Order before Redis:

1. Emergency breaker  
2. Fraud / geo  
3. Schedule, placement, L3, device, consent  
4. ML boost snapshot  
5. `UnifiedFilter` / local quanta path  

---

## Runtime limits

| Setting | Tracker | Processor |
| :--- | :--- | :--- |
| `GOMEMLIMIT` | 700 MiB | 1500 MiB |
| `GOGC` | 300 | 100 |

Separate containers recommended. Redis timeout + circuit breaker cap goroutine growth under shard latency.

---

## Wire DFA benchmarks

`internal/ingestion/http_dfa_bench_test.go`. CI: 0 allocs/op.

| Benchmark | Corpus | ns/op (median) | allocs/op |
| :--- | :--- | ---: | ---: |
| `BenchmarkHTTP1DFA_Happy` | Minimal POST `/track` | ~70 | 0 |
| `BenchmarkHTTP1DFA_Worst` | Full nginx headers | ~498 | 0 |
| `BenchmarkHTTP2DFA_Happy` | 9-byte frame header | ~8.5 | 0 |
| `BenchmarkHTTP2DFA_Worst` | Full h2c `/track` | ~112 | 0 |
| `BenchmarkHTTP3DFA_Happy` | QUIC varint | ~2.2 | 0 |
| `BenchmarkHTTP3DFA_Worst` | H3 HEADERS + DATA | ~50 | 0 |

Full table: [CAPABILITIES.md](./CAPABILITIES.md). Registered in `scripts/perf-gate/perf_gate_bench.sh`.

---

## Ingress hardening and observability (M14)

| Control | Default / flag | Effect |
| :--- | :--- | :--- |
| JSON depth | `MaxJSONDepth=16`, `OrtbMaxJSONDepth=32` | Reject deep nesting at parse; 0 allocs |
| H2 spin guard | `H2_INCOMPLETE_MAX=3` | Close hostile incomplete preface; `ad_h2_hostile_disconnect_total` |
| Quanta flush | pause / SIGTERM / strict-enter | `INCRBY budget:quota` + broker return; `ad_local_quota_flush_total{reason}` |
| Lua branches | — | `filter_lua_branch_total{branch}` per return code |
| Slow EVALSHA | `FILTER_SLOW_MS=5` | slog `campaign_id` + `tier`; correlate with Redis `SLOWLOG` |

Shard-0 survival: [DEVELOPMENT.md](./DEVELOPMENT.md) §Shard-0 outage · [CAPABILITIES.md](./CAPABILITIES.md#m14--shard-0-survival-ingress-hardening-quanta-lifecycle).

---

## System concepts (reference)

| Operation | Latency |
| :--- | :--- |
| L1 cache | ~1 ns |
| L3 cache | ~12 ns |
| Atomic CAS (same core) | ~15 ns |
| DRAM | ~100 ns |
| Syscall | ~200 ns |
| Cross-AZ TCP | ~300 µs |

Rules: batch syscalls (processor flush windows); pin workers; MPSC rings instead of channels on hot paths; `LimitNOFILE` ≥ 10⁶ in production systemd units.

---

## Hot-path PR checklist

1. `make test-alloc-gate` on touched packages  
2. Benchmark delta in PR for perf-critical changes  
3. No new `interface{}` on `/track` path  
4. BCE: explicit length check before indexed loop  
5. New write paths: chaos proof per [CHAOS.md](./CHAOS.md) R10  

Reference implementations: `ingress_quota.go`, `fraud_stream_queue.go`, `unified_filter.go`, `http1_fsm.go`.
