# `pkg/logger` QC report (P0-1 … P0-3)

**Host:** linux/amd64, GOMAXPROCS=12

---

## P0-3 — persist queue backpressure

### Change

| Item | Before | After |
|------|--------|-------|
| `persistCh` capacity | fixed `2` | `ComputePersistQueueDepth(cfg)` default `(4×FlushBufferSize/200)`, clamp `[64,4096]` |
| `sendBuffer` | `select` + `default` instant drop | block up to `PersistEnqueueTimeout` (default 25ms), then drop |
| shutdown | same | `sendBuffer(buf, true)` blocks until enqueued |
| metrics | `log_queue_depth` only | + `log_persist_queue_capacity`, `log_persist_queue_saturation`, `log_persist_queue_dropped_*` |
| `StartMetricsReporter` | `wg.Done` without `Add` | `wg.Add(1)` + internal goroutine; `Close()` waits |

### Config / env

| Field | Env | Default |
|-------|-----|---------|
| `PersistQueueDepth` | `LOGGER_PERSIST_QUEUE_DEPTH` | `0` = auto |
| `PersistEnqueueTimeout` | `LOGGER_PERSIST_ENQUEUE_TIMEOUT_MS` | `25` |

Default flush 256 KiB → queue depth **4096** (was 2).

---

## Test results (full package)

| Command | Result |
|---------|--------|
| `go test -count=1 ./pkg/logger/...` | **PASS** |
| `go test -race -count=1 ./pkg/logger/...` | **PASS** |
| `go build ./cmd/tracker/... ./cmd/processor/...` | **PASS** |

| Test | Covers |
|------|--------|
| `TestComputePersistQueueDepth` | auto / explicit / cap |
| `TestSendBufferEnqueueTimeout` | full queue + 2ms timeout → drop counters |
| `TestLogShardMPSCConcurrent` | P0-1 + P0-2 |
| `TestLogShardMPSCUniqueLines` | no lost lines under MPSC |
| `TestLoggerZeroAlloc` | hot path allocs |
| `TestLoggerRingBufferOverflow` | `ringUsable` |
| `TestLoggerDiskDegradationEmergency` | degraded shed |
| `TestLoggerRotation` | segment rotate |

---

## Benchmark (`-benchmem`, 3×)

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| `BenchmarkLoggerWriteToShard` | ~5–6 | 0 | 0 |
| `BenchmarkLogShardWriteMPSC` | ~5k | 0–1 | 0 |

```bash
go test -bench='Benchmark(LoggerWriteToShard|LogShardWriteMPSC)' -benchmem -count=3 ./pkg/logger/...
```

---

## Escape (`-gcflags='-m=2'`)

| Symbol | `data` escapes | Inline |
|--------|----------------|--------|
| `(*LogShard).Write` | no | no |
| `(*Logger).WriteToShard` | no | yes |
| `sendBuffer` | — | no |

---

## P0-1 (reference)

`allocCursor` CAS + ordered `writeCursor` publish (MPSC reservation).

## P0-2 (reference)

`ready atomic.Uint32` gate; `ringUsable = RingCapacity - 1`.

---

## Open

| ID | Item |
|----|------|
| P0-4 | priority-0 shed when `diskDegraded` |
| perf | publish-wait spin; optional multi-persister |
| ops | load-test 50k RPS + CH 50k flush with new queue metrics |
