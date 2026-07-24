package ingestion

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/metrics"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// isFraudTelemetry routes fraud signals and ghost events to the fraud_events table.
func isFraudTelemetry(e *campaignmodel.Event) bool {
	if e == nil {
		return false
	}
	if e.Type == fraudAggregateEventType {
		return false
	}
	return e.GhostEvent || e.FraudReason != "" || e.FraudScore > 0
}

const fraudAggregateEventType = "fraud_aggregate"

// isFraudAggregateSpike routes M11 subnet/reason aggregate windows to fraud_aggregate_spikes.
func isFraudAggregateSpike(e *campaignmodel.Event) bool {
	return e != nil && e.Type == fraudAggregateEventType
}

// fraudGhostFlag maps ghost_event to ClickHouse UInt8 without per-row heap allocation.
func fraudGhostFlag(e *campaignmodel.Event) uint8 {
	if e.GhostEvent {
		return 1
	}
	return 0
}

// fraudAggregateFields reads count and window_ms stashed in ClickID/UserID by parseMessage.
func fraudAggregateFields(e *campaignmodel.Event) (uint64, uint32) {
	var count uint64
	var windowMs uint32
	if e.ClickID != "" {
		if n, err := strconv.ParseUint(e.ClickID, 10, 64); err == nil {
			count = n
		}
	}
	if e.UserID != "" {
		if n, err := strconv.ParseUint(e.UserID, 10, 32); err == nil {
			windowMs = uint32(n)
		}
	}
	return count, windowMs
}

// slicePool recycles event batch slices for ClickHouse table routing.
var slicePool = sync.Pool{
	New: func() any {
		s := make([]*campaignmodel.Event, 0, 20000)
		return &s
	},
}

// ClickHouseStore batches telemetry writes and blocks until ClickHouse or the mmap WAL confirms durability.
type ClickHouseStore struct {
	conn          driver.Conn
	writeTimeout  time.Duration
	spool         *CHSpool
	chGate        *ProcessorChGate
	ctx           context.Context
	cancel        context.CancelFunc
	replayWg      sync.WaitGroup
	replayRunning atomic.Bool
}

// NewClickHouseStore starts the spool replayer when spoolDir is non-empty.
func NewClickHouseStore(conn driver.Conn, writeTimeout time.Duration, spoolDir string, spoolCfg CHSpoolConfig, chGate *ProcessorChGate) *ClickHouseStore {
	ctx, cancel := context.WithCancel(context.Background())
	chStore := &ClickHouseStore{
		conn:         conn,
		writeTimeout: writeTimeout,
		chGate:       chGate,
		ctx:          ctx,
		cancel:       cancel,
	}
	if spoolDir != "" {
		spool, err := OpenCHSpoolWithConfig(spoolDir, spoolCfg)
		if err != nil {
			slog.Error("failed to open clickhouse spool", "error", err, "dir", spoolDir)
		} else {
			chStore.spool = spool
			chStore.startSpoolReplayer()
		}
	}
	return chStore
}

// StoreBatch blocks until ClickHouse acknowledges the batch or the batch is fsynced to the mmap WAL.
func (chStore *ClickHouseStore) StoreBatch(ctx context.Context, events []*campaignmodel.Event) error {
	if len(events) == 0 {
		return nil
	}

	if chStore.chGate != nil {
		if err := chStore.chGate.Acquire(ctx); err != nil {
			return err
		}
		defer chStore.chGate.Release()
	}

	token := chStore.getDeduplicationToken(ctx, events)
	var err error
	waitTime := InitialWait

	for i := 0; i <= MaxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(ctx, chStore.writeTimeout)
		err = chStore.insertToClickHouse(dbCtx, events)
		cancel()

		if err == nil {
			return nil
		}

		if i < MaxRetries {
			timer := time.NewTimer(waitTime)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				waitTime *= 2
				if waitTime > MaxWait {
					waitTime = MaxWait
				}
			}
		}
	}

	if chStore.spool == nil {
		metrics.DbWriteErrors.WithLabelValues("clickhouse").Inc()
		return err
	}

	if spoolErr := chStore.spool.AppendDurably(token, events); spoolErr != nil {
		metrics.DbWriteErrors.WithLabelValues("clickhouse_spool").Inc()
		return fmt.Errorf("clickhouse write failed and spool append failed: ch=%w spool=%w", err, spoolErr)
	}

	metrics.CHSpoolAppendTotal.Inc()
	slog.Warn("clickhouse unavailable, batch spooled to mmap WAL", "events", len(events), "token", token)
	return nil
}

// startSpoolReplayer drains mmap WAL entries to ClickHouse when the database recovers.
func (chStore *ClickHouseStore) startSpoolReplayer() {
	if chStore.spool == nil || chStore.replayRunning.Swap(true) {
		return
	}
	chStore.replayWg.Add(1)
	go func() {
		defer chStore.replayWg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-chStore.ctx.Done():
				return
			case <-ticker.C:
				chStore.replaySpoolOnce()
			}
		}
	}()
}

// replaySpoolOnce replays the oldest WAL record and releases it when ClickHouse accepts the batch.
func (chStore *ClickHouseStore) replaySpoolOnce() {
	if chStore.spool == nil {
		return
	}
	records, err := chStore.spool.Scan()
	if err != nil || len(records) == 0 {
		return
	}

	rec := records[0]
	ctx, cancel := context.WithTimeout(chStore.ctx, chStore.writeTimeout)
	ctx = context.WithValue(ctx, campaignmodel.DeduplicationTokenKey, rec.DedupToken)
	insertErr := chStore.insertToClickHouse(ctx, rec.Events)
	cancel()
	if insertErr != nil {
		for _, e := range rec.Events {
			campaignmodel.EventPool.Put(e)
		}
		return
	}
	for _, e := range rec.Events {
		campaignmodel.EventPool.Put(e)
	}
	if err := chStore.spool.ReleaseRecord(rec); err != nil {
		slog.Error("failed to release ch spool record", "error", err, "offset", rec.EndOffset)
		return
	}
	metrics.CHSpoolReplayTotal.Inc()
}

// RecoverSpool replays pending WAL records synchronously; used on processor startup.
func (chStore *ClickHouseStore) RecoverSpool(ctx context.Context) error {
	if chStore.spool == nil {
		return nil
	}
	for {
		records, err := chStore.spool.Scan()
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		rec := records[0]
		replayCtx, cancel := context.WithTimeout(ctx, chStore.writeTimeout)
		replayCtx = context.WithValue(replayCtx, campaignmodel.DeduplicationTokenKey, rec.DedupToken)
		insertErr := chStore.insertToClickHouse(replayCtx, rec.Events)
		cancel()
		if insertErr != nil {
			for _, e := range rec.Events {
				campaignmodel.EventPool.Put(e)
			}
			return insertErr
		}
		for _, e := range rec.Events {
			campaignmodel.EventPool.Put(e)
		}
		if err := chStore.spool.ReleaseRecord(rec); err != nil {
			return err
		}
		metrics.CHSpoolReplayTotal.Inc()
	}
}

// getDeduplicationToken supplies ClickHouse insert deduplication for at-least-once retries.
func (chStore *ClickHouseStore) getDeduplicationToken(ctx context.Context, events []*campaignmodel.Event) string {
	if token, ok := ctx.Value(campaignmodel.DeduplicationTokenKey).(string); ok && token != "" {
		return token
	}
	if len(events) == 0 {
		return ""
	}
	h := sha256.New()
	for _, e := range events {
		h.Write([]byte(e.ClickID))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(e.CreatedAt.UnixNano()))
		h.Write(buf[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// insertTable sends one prepared batch to a single ClickHouse table.
func (chStore *ClickHouseStore) insertTable(ctx context.Context, table string, evts []*campaignmodel.Event, isFraud bool) error {
	start := time.Now()

	token := chStore.getDeduplicationToken(ctx, evts)
	query := fmt.Sprintf("INSERT INTO %s", table)
	if token != "" {
		query = fmt.Sprintf("INSERT INTO %s SETTINGS insert_deduplicate=1, insert_deduplication_token='%s'", table, token)
	}

	batch, err := chStore.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare batch %s: %w", table, err)
	}

	for _, e := range evts {
		if table == "fraud_aggregate_spikes" {
			count, windowMs := fraudAggregateFields(e)
			err = batch.Append(
				e.IP,
				e.FraudReason,
				count,
				windowMs,
				e.CreatedAt,
			)
		} else if isFraud {
			err = batch.Append(
				e.ClickID,
				e.CampaignID,
				e.UserID,
				e.Type,
				e.IP,
				e.UA,
				unsafeString(e.Payload),
				e.FraudReason,
				e.FraudScore,
				fraudGhostFlag(e),
				e.CreatedAt,
			)
		} else if table == "clicks" {
			err = batch.Append(
				e.ClickID,
				e.CampaignID,
				e.PlacementID,
				e.IP,
				e.UA,
				e.TLSHash,
				unsafeString(e.Payload),
				e.CreatedAt,
			)
		} else {
			err = batch.Append(
				e.ClickID,
				e.CampaignID,
				e.PlacementID,
				e.IP,
				e.UA,
				unsafeString(e.Payload),
				e.CreatedAt,
			)
		}
		if err != nil {
			return fmt.Errorf("append %s: %w", table, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send %s: %w", table, err)
	}

	duration := time.Since(start).Seconds()
	metrics.DbWriteDuration.WithLabelValues("clickhouse").Observe(duration)

	return nil
}

// insertToClickHouse writes a multi-table batch synchronously.
func (chStore *ClickHouseStore) insertToClickHouse(ctx context.Context, events []*campaignmodel.Event) error {
	start := time.Now()

	pImps := slicePool.Get().(*[]*campaignmodel.Event)
	pClicks := slicePool.Get().(*[]*campaignmodel.Event)
	pConvs := slicePool.Get().(*[]*campaignmodel.Event)
	pFraud := slicePool.Get().(*[]*campaignmodel.Event)
	pAgg := slicePool.Get().(*[]*campaignmodel.Event)

	defer func() {
		for i := range *pImps {
			(*pImps)[i] = nil
		}
		*pImps = (*pImps)[:0]

		for i := range *pClicks {
			(*pClicks)[i] = nil
		}
		*pClicks = (*pClicks)[:0]

		for i := range *pConvs {
			(*pConvs)[i] = nil
		}
		*pConvs = (*pConvs)[:0]

		for i := range *pFraud {
			(*pFraud)[i] = nil
		}
		*pFraud = (*pFraud)[:0]

		for i := range *pAgg {
			(*pAgg)[i] = nil
		}
		*pAgg = (*pAgg)[:0]

		if cap(*pImps) <= 100000 {
			slicePool.Put(pImps)
		}
		if cap(*pClicks) <= 100000 {
			slicePool.Put(pClicks)
		}
		if cap(*pConvs) <= 100000 {
			slicePool.Put(pConvs)
		}
		if cap(*pFraud) <= 100000 {
			slicePool.Put(pFraud)
		}
		if cap(*pAgg) <= 100000 {
			slicePool.Put(pAgg)
		}
	}()

	imps := *pImps
	clicks := *pClicks
	convs := *pConvs
	fraud := *pFraud
	agg := *pAgg

	for i := range events {
		e := events[i]
		if isFraudAggregateSpike(e) {
			agg = append(agg, e)
			continue
		}
		if isFraudTelemetry(e) {
			fraud = append(fraud, e)
			continue
		}

		switch e.Type {
		case "impression":
			imps = append(imps, e)
		case "click":
			clicks = append(clicks, e)
		case "conversion":
			convs = append(convs, e)
		}
	}

	*pImps, *pClicks, *pConvs, *pFraud, *pAgg = imps, clicks, convs, fraud, agg

	insert := func(table string, evts []*campaignmodel.Event, isFraud bool) error {
		if len(evts) == 0 {
			return nil
		}
		return chStore.insertTable(ctx, table, evts, isFraud)
	}

	if err := insert("impressions", imps, false); err != nil {
		return err
	}
	if err := insert("clicks", clicks, false); err != nil {
		return err
	}
	if err := insert("conversions", convs, false); err != nil {
		return err
	}
	if err := insert("fraud_events", fraud, true); err != nil {
		return err
	}
	if err := insert("fraud_aggregate_spikes", agg, false); err != nil {
		return err
	}

	duration := time.Since(start).Seconds()
	metrics.DbWriteDuration.WithLabelValues("clickhouse").Observe(duration)

	return nil
}

// Close stops the spool replayer and closes connections.
func (chStore *ClickHouseStore) Close() error {
	chStore.cancel()
	chStore.replayWg.Wait()
	if chStore.spool != nil {
		_ = chStore.spool.Close()
	}
	return chStore.conn.Close()
}

// SetChGate attaches the processor ClickHouse write gate for SEM-P5 backpressure.
func (chStore *ClickHouseStore) SetChGate(gate *ProcessorChGate) {
	chStore.chGate = gate
}

// SetSpool exposes the mmap WAL for tests.
func (chStore *ClickHouseStore) SetSpool(spool *CHSpool) {
	chStore.spool = spool
}

// Spool returns the active mmap WAL when configured.
func (chStore *ClickHouseStore) Spool() *CHSpool {
	return chStore.spool
}
