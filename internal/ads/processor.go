package ads

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/ads/pb"
	"espx/internal/domain"
	"espx/internal/metrics"
	"espx/pkg/logger"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// StreamConsumer reads accepted events from Redis streams and persists them in batches.
type StreamConsumer struct {
	store              domain.EventStore
	rdb                redis.UniversalClient
	streamName         string
	groupName          string
	consumerID         string
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	startMu            sync.Mutex
	flushInt           time.Duration
	writeTimeout       time.Duration
	retryInitWait      time.Duration
	retryMaxWait       time.Duration
	streamMinIdle      time.Duration
	drainTimeout       time.Duration
	batchSize          int
	maxWorkers         int
	maxRetries         int
	started            bool
	cb                 *CircuitBreaker
	logger             *logger.Logger
	auditLogSeq        atomic.Uint64
	auditLogSampleMask uint64
	dlqStreamName      string
}

// SetLogger attaches the audit log writer invoked after successful batch stores.
func (consumer *StreamConsumer) SetLogger(l *logger.Logger) {
	consumer.logger = l
}

// SetAuditLogSampleMask configures audit log downsampling for the consumer path.
func (consumer *StreamConsumer) SetAuditLogSampleMask(mask int) {
	consumer.auditLogSampleMask = auditLogSampleMaskFromConfig(mask)
}

// SetDLQStream overrides the dead-letter stream name for fraud or billing consumers.
func (consumer *StreamConsumer) SetDLQStream(name string) {
	consumer.dlqStreamName = name
}

// dlqStream resolves the DLQ stream from an explicit override or the main stream name.
func (consumer *StreamConsumer) dlqStream() string {
	if consumer.dlqStreamName != "" {
		return consumer.dlqStreamName
	}
	const suffix = ":stream"
	if strings.HasSuffix(consumer.streamName, suffix) {
		return consumer.streamName[:len(consumer.streamName)-len(suffix)] + ":dlq"
	}
	return "ad:events:dlq"
}

// CircuitBreakerState exposes the store circuit state for chaos and integration tests.
func (consumer *StreamConsumer) CircuitBreakerState() CircuitState {
	if consumer == nil || consumer.cb == nil {
		return CircuitClosed
	}
	return consumer.cb.State()
}

// logBufPool recycles audit log marshal buffers in the consumer path.
var logBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// adLogRecordPool recycles protobuf audit records written after successful stores.
var adLogRecordPool = sync.Pool{
	New: func() any {
		return &pb.AdLogRecord{}
	},
}

// NewStreamConsumer creates a sharded stream reader with unique consumer ids per replica.
func NewStreamConsumer(
	store domain.EventStore,
	rdb redis.UniversalClient,
	streamName, groupName, consumerID string,
	batchSize int,
	maxWorkers int,
	flushInt, writeTimeout time.Duration,
	retryInitWait, retryMaxWait time.Duration,
	maxRetries int,
	streamMinIdle time.Duration,
	drainTimeout time.Duration,
) *StreamConsumer {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	uniqueConsumerID := fmt.Sprintf("%s-%s-%s", consumerID, hostname, uuid.NewString()[:8])

	return &StreamConsumer{
		store:              store,
		rdb:                rdb,
		streamName:         streamName,
		groupName:          groupName,
		consumerID:         uniqueConsumerID,
		batchSize:          batchSize,
		flushInt:           flushInt,
		writeTimeout:       writeTimeout,
		maxWorkers:         maxWorkers,
		retryInitWait:      retryInitWait,
		retryMaxWait:       retryMaxWait,
		maxRetries:         maxRetries,
		streamMinIdle:      streamMinIdle,
		drainTimeout:       drainTimeout,
		cb:                 NewCircuitBreaker(maxRetries, retryMaxWait*2),
		auditLogSampleMask: auditLogSampleMaskDefault,
	}
}

// Start launches consumer workers, pending recovery, and maintenance goroutines.
func (consumer *StreamConsumer) Start(ctx context.Context) {
	consumer.startMu.Lock()
	defer consumer.startMu.Unlock()
	if consumer.started {
		return
	}
	consumer.started = true

	procCtx, cancel := context.WithCancel(ctx)
	consumer.cancel = cancel
	err := consumer.rdb.XGroupCreateMkStream(ctx, consumer.streamName, consumer.groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		slog.Error("failed to create consumer group", "error", err, "stream", consumer.streamName, "group", consumer.groupName)
	}

	for i := 0; i < consumer.maxWorkers; i++ {
		consumer.wg.Add(1)
		go func(workerIdx int) {
			defer consumer.wg.Done()
			consumer.worker(procCtx, workerIdx)
		}(i)
	}

	consumer.wg.Add(1)
	go func() {
		defer consumer.wg.Done()
		consumer.janitor(procCtx)
	}()

	consumer.wg.Add(1)
	go func() {
		defer consumer.wg.Done()
		consumer.dlqMonitor(procCtx)
	}()
}

// Close cancels the consumer context without waiting for workers.
func (consumer *StreamConsumer) Close() {
	if consumer.cancel != nil {
		consumer.cancel()
	}
}

// Wait blocks until all consumer goroutines exit or the context is cancelled.
func (consumer *StreamConsumer) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		consumer.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// workerConsumerID names each worker in the consumer group so PEL recovery stays shard-local.
func (consumer *StreamConsumer) workerConsumerID(workerIdx int) string {
	return fmt.Sprintf("%s-w%d", consumer.consumerID, workerIdx)
}

// worker reads stream batches, flushes to the store, and handles shutdown drain.
func (consumer *StreamConsumer) worker(ctx context.Context, workerIdx int) {
	workerID := consumer.workerConsumerID(workerIdx)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker panic recovered - exiting process", "error", r, "worker", workerID)
			os.Exit(1)
		}
	}()

	initCtx, initCancel := context.WithTimeout(context.Background(), consumer.writeTimeout*2)
	consumer.recoverPending(initCtx, workerID)
	initCancel()

	batch := make([]*domain.Event, 0, consumer.batchSize)
	msgIDs := make([]string, 0, consumer.batchSize)

	retryWait := consumer.retryInitWait
	retryCount := 0
	lastFlush := time.Now()

	xreadArgs := &redis.XReadGroupArgs{
		Group:    consumer.groupName,
		Consumer: workerID,
		Streams:  []string{consumer.streamName, ">"},
	}

	for {
		select {
		case <-ctx.Done():
			drainCtx, drainCancel := context.WithTimeout(context.Background(), consumer.drainTimeout)
			if len(batch) > 0 {
				if err := consumer.flushBatch(drainCtx, batch, msgIDs, workerID); err == nil {
					for _, e := range batch {
						domain.EventPool.Put(e)
					}
				} else {
					slog.Error("drain flush of existing batch failed, GC will reclaim objects", "error", err, "group", consumer.groupName, "worker", workerID)
				}
				batch = batch[:0]
				msgIDs = msgIDs[:0]
			}

			consumer.drainNewMessages(drainCtx, workerID)
			consumer.recoverPending(drainCtx, workerID)
			drainCancel()
			return
		default:
		}

		readCount := int64(consumer.batchSize - len(batch))
		if readCount <= 0 {
			consumer.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
			lastFlush = time.Now()
			continue
		}

		var blockTime time.Duration
		if len(batch) == 0 {
			blockTime = 200 * time.Millisecond
		} else {
			elapsed := time.Since(lastFlush)
			if elapsed >= consumer.flushInt {
				consumer.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
				lastFlush = time.Now()
				continue
			}
			blockTime = consumer.flushInt - elapsed
			if blockTime > 200*time.Millisecond {
				blockTime = 200 * time.Millisecond
			}
		}

		xreadArgs.Count = readCount
		xreadArgs.Block = blockTime
		streams, err := consumer.rdb.XReadGroup(ctx, xreadArgs).Result()

		if err != nil {
			if err == redis.Nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				if len(batch) > 0 && time.Since(lastFlush) >= consumer.flushInt {
					consumer.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
					lastFlush = time.Now()
				}
			} else {
				slog.Error("failed to read from redis stream", "error", err)
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			continue
		}

		hadEmptyBatch := len(batch) == 0

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				batch = append(batch, consumer.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}
		}

		if hadEmptyBatch && len(batch) > 0 {
			lastFlush = time.Now()
		}

		if len(batch) >= consumer.batchSize || time.Since(lastFlush) >= consumer.flushInt {
			consumer.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
			lastFlush = time.Now()
		}
	}
}

// recordSuccess clears the store circuit breaker after a successful flush.
func (consumer *StreamConsumer) recordSuccess(workerID string) {
	consumer.cb.RecordSuccess(workerID)
	metrics.CircuitBreakerState.WithLabelValues(consumer.groupName).Set(float64(consumer.cb.State()))
}

// recordFailure opens the store circuit breaker after a failed flush.
func (consumer *StreamConsumer) recordFailure(workerID string) {
	consumer.cb.RecordFailure(workerID)
	metrics.CircuitBreakerState.WithLabelValues(consumer.groupName).Set(float64(consumer.cb.State()))
}

// recordCancellation treats cancelled flushes as circuit failures during half-open probes.
func (consumer *StreamConsumer) recordCancellation(workerID string) {
	consumer.cb.RecordCancellation(workerID)
	metrics.CircuitBreakerState.WithLabelValues(consumer.groupName).Set(float64(consumer.cb.State()))
}

// tryFlush persists the current batch with retry, poison-pill splitting, and DLQ routing.
func (consumer *StreamConsumer) tryFlush(ctx context.Context, batch *[]*domain.Event, msgIDs *[]string, retryCount *int, workerID string, ticker *time.Ticker, retryWait *time.Duration) {
	if !consumer.cb.Allow() {
		wait := consumer.cb.WaitDuration()
		if wait <= 0 {
			wait = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		return
	}
	err := consumer.flushBatch(ctx, *batch, *msgIDs, workerID)
	if err == nil {
		consumer.recordSuccess(workerID)
		_ = consumer.rdb.HDel(ctx, "ad:events:retries", (*msgIDs)...).Err()
		for _, e := range *batch {
			domain.EventPool.Put(e)
		}
		*batch = (*batch)[:0]
		*msgIDs = (*msgIDs)[:0]
		if ticker != nil {
			ticker.Reset(consumer.flushInt)
		}
		*retryWait = 100 * time.Millisecond
		*retryCount = 0
		return
	}

	if errors.Is(err, context.Canceled) {
		consumer.recordCancellation(workerID)
		return
	}

	*retryCount++
	consumer.recordFailure(workerID)

	pipe := consumer.rdb.Pipeline()
	incrCmds := make([]*redis.IntCmd, len(*msgIDs))
	for i, id := range *msgIDs {
		incrCmds[i] = pipe.HIncrBy(ctx, "ad:events:retries", id, 1)
	}
	_, _ = pipe.Exec(ctx)

	hasPoisonPill := false
	maxIncr := int64(0)
	for i := range *msgIDs {
		cVal, _ := incrCmds[i].Result()
		if cVal > maxIncr {
			maxIncr = cVal
		}
		if cVal > int64(consumer.maxRetries) {
			hasPoisonPill = true
		}
	}

	if maxIncr > int64(*retryCount) {
		*retryCount = int(maxIncr)
	}

	if hasPoisonPill {
		slog.Error("poison pill detected, decomposing batch", "error", err, "group", consumer.groupName, "worker", workerID)

		failedIndices := make([]int, 0, len(*batch))
		successfulMsgIDs := make([]string, 0, len(*batch))
		singleBatch := make([]*domain.Event, 1)

		for i, e := range *batch {
			if ctx.Err() != nil {
				for j := i; j < len(*batch); j++ {
					failedIndices = append(failedIndices, j)
				}
				break
			}

			singleBatch[0] = e
			singleCtx, singleCancel := context.WithTimeout(ctx, consumer.writeTimeout)
			if len(*msgIDs) > i {
				singleCtx = context.WithValue(singleCtx, domain.DeduplicationTokenKey, (*msgIDs)[i])
			}
			if singleErr := consumer.store.StoreBatch(singleCtx, singleBatch); singleErr != nil {
				singleCancel()
				failedIndices = append(failedIndices, i)
			} else {
				singleCancel()
				successfulMsgIDs = append(successfulMsgIDs, (*msgIDs)[i])
			}
		}

		if len(successfulMsgIDs) > 0 {
			ackCtx, ackCancel := context.WithTimeout(context.Background(), consumer.writeTimeout)
			_ = consumer.rdb.XAck(ackCtx, consumer.streamName, consumer.groupName, successfulMsgIDs...).Err()
			_ = consumer.rdb.HDel(ackCtx, "ad:events:retries", successfulMsgIDs...).Err()
			ackCancel()
		}

		if len(failedIndices) > 0 {
			failedBatch := make([]*domain.Event, 0, len(failedIndices))
			failedMsgIDs := make([]string, 0, len(failedIndices))
			for _, i := range failedIndices {
				failedBatch = append(failedBatch, (*batch)[i])
				failedMsgIDs = append(failedMsgIDs, (*msgIDs)[i])
			}

			execErr := consumer.moveToDLQ(ctx, failedBatch, failedMsgIDs, workerID, *retryCount, fmt.Errorf("batch decomposed: %w", err))

			if execErr != nil {
				slog.Error("failed to exec dlq pipeline, retaining in PEL", "error", execErr, "group", consumer.groupName)
				newBatch := (*batch)[:0]
				newMsgIDs := (*msgIDs)[:0]
				for _, i := range failedIndices {
					newBatch = append(newBatch, (*batch)[i])
					newMsgIDs = append(newMsgIDs, (*msgIDs)[i])
				}
				fiIdx := 0
				for i, e := range *batch {
					if fiIdx < len(failedIndices) && i == failedIndices[fiIdx] {
						fiIdx++
					} else {
						domain.EventPool.Put(e)
					}
				}
				*batch = newBatch
				*msgIDs = newMsgIDs
				return
			}
		}

		for _, e := range *batch {
			domain.EventPool.Put(e)
		}
		*batch = (*batch)[:0]
		*msgIDs = (*msgIDs)[:0]
		if ticker != nil {
			ticker.Reset(consumer.flushInt)
		}
		*retryWait = 100 * time.Millisecond
		*retryCount = 0
	} else {
		select {
		case <-ctx.Done():
			return
		case <-time.After(*retryWait):
		}
		*retryWait *= 2
		if *retryWait > consumer.retryMaxWait {
			*retryWait = consumer.retryMaxWait
		}
	}
}

// dlqEventPool recycles DLQ protobuf payloads before writing to the dead letter stream.
var (
	dlqEventPool = sync.Pool{
		New: func() any {
			return new(pb.AdDLQEvent)
		},
	}
	dlqValuesPool = sync.Pool{
		New: func() any {
			slice := make([]any, 2)
			slice[0] = "d"
			return &slice
		},
	}
)

// moveToDLQ writes failed messages to the DLQ stream and acks them from the main PEL.
func (consumer *StreamConsumer) moveToDLQ(ctx context.Context, batch []*domain.Event, msgIDs []string, workerID string, retryCount int, err error) error {
	errStr := err.Error()

	pipeWrite := consumer.rdb.Pipeline()

	writtenMsgIDs := make([]string, 0, len(batch))
	valuesPtrs := make([]*[]any, 0, len(batch))
	bufPtrs := make([]*[]byte, 0, len(batch))
	wrapPtrs := make([]*ByteSliceValue, 0, len(batch))
	defer func() {
		for _, ptr := range valuesPtrs {
			dlqValuesPool.Put(ptr)
		}
		for _, ptr := range bufPtrs {
			byteBufPool.Put(ptr)
		}
		for _, ptr := range wrapPtrs {
			byteSliceValuePool.Put(ptr)
		}
	}()

	execCtx, execCancel := context.WithTimeout(context.Background(), consumer.writeTimeout)
	defer execCancel()

	for i, e := range batch {
		pbDLQ := dlqEventPool.Get().(*pb.AdDLQEvent)
		if pbDLQ.OriginalEvent == nil {
			pbDLQ.OriginalEvent = new(pb.AdStreamEvent)
		} else {
			DeepResetAdStreamEvent(pbDLQ.OriginalEvent)
		}
		pbDLQ.Error = append(pbDLQ.Error[:0], errStr...)
		pbDLQ.OriginalId = append(pbDLQ.OriginalId[:0], msgIDs[i]...)
		pbDLQ.FailedAtUnix = time.Now().Unix()
		pbDLQ.WorkerId = append(pbDLQ.WorkerId[:0], workerID...)
		pbDLQ.RetryCount = int32(retryCount)

		pbDLQ.OriginalEvent.ClickId = append(pbDLQ.OriginalEvent.ClickId[:0], e.ClickID...)
		pbDLQ.OriginalEvent.CampaignId = append(pbDLQ.OriginalEvent.CampaignId[:0], e.CampaignID[:]...)
		pbDLQ.OriginalEvent.EventType = append(pbDLQ.OriginalEvent.EventType[:0], e.Type...)
		pbDLQ.OriginalEvent.Payload = append(pbDLQ.OriginalEvent.Payload[:0], e.Payload...)
		pbDLQ.OriginalEvent.Ip = append(pbDLQ.OriginalEvent.Ip[:0], e.IP...)
		pbDLQ.OriginalEvent.Ua = append(pbDLQ.OriginalEvent.Ua[:0], e.UA...)
		pbDLQ.OriginalEvent.CreatedAtUnix = e.CreatedAt.Unix()

		size := pbDLQ.SizeVT()
		bufPtr := byteBufPool.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) < size {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}

		n, marshalErr := pbDLQ.MarshalToSizedBufferVT(buf)
		if marshalErr != nil {
			slog.Error("failed to marshal DLQ event", "error", marshalErr)
			DeepResetAdDLQEvent(pbDLQ)
			dlqEventPool.Put(pbDLQ)
			*bufPtr = buf
			byteBufPool.Put(bufPtr)
			continue
		}

		data := buf[:n]
		*bufPtr = buf
		bufPtrs = append(bufPtrs, bufPtr)

		DeepResetAdDLQEvent(pbDLQ)
		dlqEventPool.Put(pbDLQ)
		writtenMsgIDs = append(writtenMsgIDs, msgIDs[i])

		valuesPtr := dlqValuesPool.Get().(*[]any)
		values := *valuesPtr

		wrap := byteSliceValuePool.Get().(*ByteSliceValue)
		wrap.b = data
		values[1] = wrap
		wrapPtrs = append(wrapPtrs, wrap)

		valuesPtrs = append(valuesPtrs, valuesPtr)

		pipeWrite.XAdd(execCtx, &redis.XAddArgs{
			Stream: consumer.dlqStream(),
			MaxLen: 100000,
			Approx: true,
			Values: values,
		})
	}

	if len(writtenMsgIDs) == 0 {
		return nil
	}

	cmders, execErr := pipeWrite.Exec(execCtx)

	var hasError bool
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		slog.Error("DLQ write pipeline returned error", "error", execErr)
		hasError = true
	}

	pipeAck := consumer.rdb.Pipeline()
	ackedMsgIDs := make([]string, 0, len(batch))

	ackCtx, ackCancel := context.WithTimeout(context.Background(), consumer.writeTimeout)
	defer ackCancel()

	for i, cmder := range cmders {
		if cmder.Err() == nil {
			msgID := writtenMsgIDs[i]
			pipeAck.XAck(ackCtx, consumer.streamName, consumer.groupName, msgID)
			pipeAck.XDel(ackCtx, consumer.streamName, msgID)
			ackedMsgIDs = append(ackedMsgIDs, msgID)
		} else {
			slog.Error("individual DLQ write failed", "error", cmder.Err(), "msgID", writtenMsgIDs[i])
			hasError = true
		}
	}

	if len(ackedMsgIDs) > 0 {
		_, ackErr := pipeAck.Exec(ackCtx)
		if ackErr != nil {
			slog.Error("DLQ ack/del pipeline failed", "error", ackErr)
			return ackErr
		}
	}

	if hasError || len(ackedMsgIDs) < len(writtenMsgIDs) {
		return fmt.Errorf("DLQ write partial failure: wrote %d of %d messages", len(ackedMsgIDs), len(writtenMsgIDs))
	}

	return nil
}

// parseMessage rebuilds pooled domain events from stream entries for batched store writes.
func (consumer *StreamConsumer) parseMessage(id string, values map[string]interface{}) *domain.Event {
	event := domain.EventPool.Get().(*domain.Event)
	event.Reset()

	if rawBytesStr, ok := values["d"].(string); ok {
		pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
		DeepResetAdStreamEvent(pbEvt)

		buf := UnsafeBytes(rawBytesStr)
		if err := pbEvt.UnmarshalVT(buf); err == nil {
			totalLen := len(pbEvt.ClickId) + len(pbEvt.EventType) + len(pbEvt.Ip) + len(pbEvt.Ua)
			if cap(event.StringBuffer) < totalLen {
				event.StringBuffer = make([]byte, 0, totalLen+128)
			} else {
				event.StringBuffer = event.StringBuffer[:0]
			}

			event.StringBuffer = append(event.StringBuffer, pbEvt.ClickId...)
			event.ClickID = unsafeString(event.StringBuffer[len(event.StringBuffer)-len(pbEvt.ClickId):])

			event.StringBuffer = append(event.StringBuffer, pbEvt.EventType...)
			event.Type = unsafeString(event.StringBuffer[len(event.StringBuffer)-len(pbEvt.EventType):])

			event.StringBuffer = append(event.StringBuffer, pbEvt.Ip...)
			event.IP = unsafeString(event.StringBuffer[len(event.StringBuffer)-len(pbEvt.Ip):])

			event.StringBuffer = append(event.StringBuffer, pbEvt.Ua...)
			event.UA = unsafeString(event.StringBuffer[len(event.StringBuffer)-len(pbEvt.Ua):])

			_ = ParseUUID(pbEvt.CampaignId, &event.CampaignID)
			event.Payload = append(event.Payload[:0], pbEvt.Payload...)
			if len(pbEvt.FraudReason) > 0 {
				event.StringBuffer = append(event.StringBuffer, pbEvt.FraudReason...)
				event.FraudReason = unsafeString(event.StringBuffer[len(event.StringBuffer)-len(pbEvt.FraudReason):])
			}
			event.FraudScore = pbEvt.FraudScore
			event.GhostEvent = pbEvt.GhostEvent
			if pbEvt.CreatedAtUnix > 0 {
				event.CreatedAt = time.Unix(pbEvt.CreatedAtUnix, 0)
			}
		} else {
			slog.Error("failed to unmarshal stream event protobuf", "error", err)
		}
		DeepResetAdStreamEvent(pbEvt)
		streamEventPool.Put(pbEvt)
	} else {
		if v, ok := values["click_id"].(string); ok {
			event.ClickID = v
		}
		if v, ok := values["campaign_id"].(string); ok {
			event.CampaignID, _ = uuid.Parse(v)
		}
		if v, ok := values["user_id"].(string); ok {
			event.UserID = v
		}
		if v, ok := values["type"].(string); ok {
			event.Type = v
		}
		if v, ok := values["payload"].(string); ok {
			event.Payload = append(event.Payload[:0], v...)
		}
		if v, ok := values["ip"].(string); ok {
			event.IP = v
		}
		if v, ok := values["ua"].(string); ok {
			event.UA = v
		}
		if v, ok := values["fraud_reason"].(string); ok {
			event.FraudReason = v
		}
		if v, ok := values["fraud_score"].(string); ok {
			if n, err := strconv.ParseUint(v, 10, 32); err == nil {
				event.FraudScore = uint32(n)
			}
		}
		if v, ok := values["ghost_event"].(string); ok {
			event.GhostEvent = v == "1" || v == "true"
		}
		if v, ok := values["created_at"].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				event.CreatedAt = t.UTC()
			} else if t, err := time.Parse(time.RFC3339, v); err == nil {
				event.CreatedAt = t.UTC()
			}
		}
	}

	if event.CreatedAt.IsZero() {
		if idx := strings.IndexByte(id, '-'); idx > 0 {
			ms, err := strconv.ParseInt(id[:idx], 10, 64)
			if err == nil {
				event.CreatedAt = time.Unix(0, ms*int64(time.Millisecond))
			}
		}
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}

	return event
}

// firstN caps stream id lists in debug logs during large batch failures.
func firstN(ids []string, n int) []string {
	if len(ids) <= n {
		return ids
	}
	return ids[:n]
}

// flushBatch stores a batch and acks Redis only after a durable write succeeds.
func (consumer *StreamConsumer) flushBatch(ctx context.Context, batch []*domain.Event, msgIDs []string, workerID string) error {
	if len(batch) == 0 {
		return nil
	}

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("flushing batch", "group", consumer.groupName, "batch_size", len(batch), "first_ids", firstN(msgIDs, 5))
	}

	storeCtx, storeCancel := context.WithTimeout(ctx, consumer.writeTimeout)
	if len(msgIDs) > 0 {
		token := fmt.Sprintf("%s_%s_%d", msgIDs[0], msgIDs[len(msgIDs)-1], len(msgIDs))
		storeCtx = context.WithValue(storeCtx, domain.DeduplicationTokenKey, token)
	}
	defer storeCancel()

	err := consumer.store.StoreBatch(storeCtx, batch)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("store failed, NOT ACKING", "error", err, "group", consumer.groupName, "batch_size", len(batch), "first_ids", firstN(msgIDs, 5))
		}
		return err
	}

	if consumer.logger != nil {
		workerIdx := 0
		if idx := strings.LastIndex(workerID, "-w"); idx != -1 {
			if val, err := strconv.Atoi(workerID[idx+2:]); err == nil {
				workerIdx = val
			}
		}
		for _, e := range batch {
			writeAuditLog(consumer.logger, &consumer.auditLogSeq, consumer.auditLogSampleMask, workerIdx, e.CreatedAt.Unix(), e.CampaignID, e.ClickID, e.Type)
		}
	}

	ackCtx, cancel := context.WithTimeout(ctx, consumer.writeTimeout)
	defer cancel()
	if err := consumer.rdb.XAck(ackCtx, consumer.streamName, consumer.groupName, msgIDs...).Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("xack failed after successful store", "error", err, "group", consumer.groupName, "batch_size", len(batch), "first_ids", firstN(msgIDs, 5))
		}
		return err
	}
	return nil
}

// recoverPending replays orphaned PEL messages owned by this consumer on startup.
func (consumer *StreamConsumer) recoverPending(ctx context.Context, consumerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			entries, err := consumer.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    consumer.groupName,
				Consumer: consumerID,
				Streams:  []string{consumer.streamName, "0"},
				Count:    int64(consumer.batchSize),
			}).Result()

			if err != nil || len(entries) == 0 || len(entries[0].Messages) == 0 {
				return
			}

			batch := make([]*domain.Event, 0, len(entries[0].Messages))
			msgIDs := make([]string, 0, len(entries[0].Messages))

			for _, msg := range entries[0].Messages {
				batch = append(batch, consumer.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}

			if err := consumer.flushBatch(ctx, batch, msgIDs, consumerID); err != nil {
				if !errors.Is(err, context.Canceled) {
					consumer.recordFailure(consumerID)
					slog.Error("recovery flush failed, moving to DLQ", "error", err, "group", consumer.groupName)
					_ = consumer.moveToDLQ(ctx, batch, msgIDs, consumerID, 1, fmt.Errorf("recovery flush failed: %w", err))
					_ = consumer.rdb.HDel(ctx, "ad:events:retries", msgIDs...).Err()
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
				return
			}
			consumer.recordSuccess(consumerID)
			_ = consumer.rdb.HDel(ctx, "ad:events:retries", msgIDs...).Err()
			for _, e := range batch {
				domain.EventPool.Put(e)
			}
		}
	}
}

// drainNewMessages flushes newly read messages during graceful shutdown.
func (consumer *StreamConsumer) drainNewMessages(ctx context.Context, consumerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := consumer.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    consumer.groupName,
				Consumer: consumerID,
				Streams:  []string{consumer.streamName, ">"},
				Count:    int64(consumer.batchSize),
				Block:    50 * time.Millisecond,
			}).Result()

			if err != nil {
				if err == redis.Nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, redis.ErrClosed) || strings.Contains(err.Error(), "client is closed") {
					return
				}
				slog.Error("drain: failed to read from stream", "error", err, "group", consumer.groupName, "worker", consumerID)
				return
			}

			if len(streams) == 0 || len(streams[0].Messages) == 0 {
				return
			}

			batch := make([]*domain.Event, 0, len(streams[0].Messages))
			msgIDs := make([]string, 0, len(streams[0].Messages))

			for _, msg := range streams[0].Messages {
				batch = append(batch, consumer.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}

			if err := consumer.flushBatch(ctx, batch, msgIDs, consumerID); err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error("drain: failed to flush batch", "error", err, "group", consumer.groupName, "worker", consumerID)
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
				return
			}

			for _, e := range batch {
				domain.EventPool.Put(e)
			}
		}
	}
}

// janitor periodically claims stuck PEL messages and retries or routes them to the DLQ.
func (consumer *StreamConsumer) janitor(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("janitor panic recovered - exiting process", "error", r)
			os.Exit(1)
		}
	}()
	ticker := time.NewTicker(consumer.streamMinIdle)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			consumer.claimStuckMessages(ctx)
		}
	}
}

// claimStuckMessages autoclaims idle PEL entries and applies retry or DLQ policy.
func (consumer *StreamConsumer) claimStuckMessages(ctx context.Context) {
	startID := "0-0"
	for {
		entries, nextID, err := consumer.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   consumer.streamName,
			Group:    consumer.groupName,
			Consumer: consumer.consumerID,
			MinIdle:  consumer.streamMinIdle,
			Start:    startID,
			Count:    int64(consumer.batchSize),
		}).Result()

		if err != nil {
			if err != redis.Nil && !errors.Is(err, context.Canceled) {
				slog.Error("autoclaim failed", "error", err, "group", consumer.groupName)
			}
			return
		}

		if len(entries) > 0 {
			pipe := consumer.rdb.Pipeline()
			incrCmds := make([]*redis.IntCmd, len(entries))
			for i, msg := range entries {
				incrCmds[i] = pipe.HIncrBy(ctx, "ad:events:retries", msg.ID, 1)
			}
			_, _ = pipe.Exec(ctx)

			batch := make([]*domain.Event, 0, len(entries))
			msgIDs := make([]string, 0, len(entries))
			var dlqBatch []*domain.Event
			var dlqMsgIDs []string
			var delMsgIDs []string

			for i, msg := range entries {
				event := consumer.parseMessage(msg.ID, msg.Values)
				count, _ := incrCmds[i].Result()
				if count > int64(consumer.maxRetries) {
					dlqBatch = append(dlqBatch, event)
					dlqMsgIDs = append(dlqMsgIDs, msg.ID)
					delMsgIDs = append(delMsgIDs, msg.ID)
				} else {
					batch = append(batch, event)
					msgIDs = append(msgIDs, msg.ID)
				}
			}

			if len(dlqBatch) > 0 {
				slog.Error("autoclaim retry limit exceeded, moving to DLQ", "group", consumer.groupName, "count", len(dlqBatch))
				_ = consumer.moveToDLQ(ctx, dlqBatch, dlqMsgIDs, "janitor", consumer.maxRetries+1, errors.New("autoclaim delivery limit exceeded"))
				for _, e := range dlqBatch {
					domain.EventPool.Put(e)
				}
				if len(delMsgIDs) > 0 {
					_ = consumer.rdb.HDel(ctx, "ad:events:retries", delMsgIDs...).Err()
				}
			}

			if len(batch) > 0 {
				if err := consumer.flushBatch(ctx, batch, msgIDs, "janitor"); err != nil {
					consumer.recordFailure("janitor")
					if !errors.Is(err, context.Canceled) {
						slog.Error("janitor flush failed", "error", err, "group", consumer.groupName)
					}
				} else {
					consumer.recordSuccess("janitor")
					_ = consumer.rdb.HDel(ctx, "ad:events:retries", msgIDs...).Err()
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
			}
		}

		if nextID == "0-0" {
			break
		}
		startID = nextID
	}
}

// dlqMonitor publishes DLQ depth metrics for alerting.
func (consumer *StreamConsumer) dlqMonitor(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("dlq monitor panic recovered - exiting process", "error", r)
			os.Exit(1)
		}
	}()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			size, err := consumer.rdb.XLen(ctx, consumer.dlqStream()).Result()
			if err != nil {
				if err != redis.Nil && !errors.Is(err, context.Canceled) {
					slog.Error("failed to get DLQ size", "error", err)
				}
				continue
			}
			metrics.DlqSize.Set(float64(size))
		}
	}
}
