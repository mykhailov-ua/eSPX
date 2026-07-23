package ingestion

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/dedup"
	"espx/internal/metrics"
	"espx/pkg/broker/client"
	"espx/pkg/dedupkey"
	"espx/pkg/logger"
)

// BrokerConsumerConfig wires processor stores to a broker topic and consumer group.
type BrokerConsumerConfig struct {
	BrokerAddr string
	RedisURL   string
	Topic      string
	Partition  uint16
	Group      string
	BatchSize  int
	FlushInt   time.Duration
	MaxBytes   uint32
	Timeout    time.Duration
	IdleWait   time.Duration
	ShadowMode bool
}

// BrokerStreamConsumer reads a broker topic and flushes batches into an EventStore.
type BrokerStreamConsumer struct {
	store        campaignmodel.EventStore
	cfg          BrokerConsumerConfig
	writeTimeout time.Duration
	maxRetries   int
	retryInit    time.Duration
	retryMax     time.Duration
	cb           *CircuitBreaker
	dedup        *dedup.Adapter
	logger       *logger.Logger
	cli          *client.Client
	auditLogSeq  atomic.Uint64
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	started      bool
	startMu      sync.Mutex
}

// NewBrokerStreamConsumer creates a cold-path broker reader for PG or CH stores.
func NewBrokerStreamConsumer(
	store campaignmodel.EventStore,
	cfg BrokerConsumerConfig,
	writeTimeout time.Duration,
	retryInit, retryMax time.Duration,
	maxRetries int,
) *BrokerStreamConsumer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInt <= 0 {
		cfg.FlushInt = 500 * time.Millisecond
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 1024 * 1024
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.IdleWait == 0 {
		cfg.IdleWait = 250 * time.Millisecond
	}
	return &BrokerStreamConsumer{
		store:        store,
		cfg:          cfg,
		writeTimeout: writeTimeout,
		maxRetries:   maxRetries,
		retryInit:    retryInit,
		retryMax:     retryMax,
		cb:           NewCircuitBreaker(maxRetries, retryMax*2),
		cli:          client.NewClient(cfg.BrokerAddr, cfg.Timeout),
	}
}

// SetDedupAdapter wires D3 v2 batch dedup for PG broker ingest (M4-15).
func (b *BrokerStreamConsumer) SetDedupAdapter(adapter *dedup.Adapter) {
	if b != nil {
		b.dedup = adapter
	}
}

// SetLogger attaches audit logging after successful broker batch writes.
func (b *BrokerStreamConsumer) SetLogger(l *logger.Logger) {
	b.logger = l
}

// Start launches the broker fetch loop until Close is called.
func (b *BrokerStreamConsumer) Start(ctx context.Context) {
	b.startMu.Lock()
	defer b.startMu.Unlock()
	if b.started {
		return
	}
	b.started = true

	if b.cfg.RedisURL != "" {
		b.cli.SetRedisURL(b.cfg.RedisURL)
	}

	runCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.run(runCtx)
	}()
}

// Close stops the broker consumer loop.
func (b *BrokerStreamConsumer) Close() {
	if b.cancel != nil {
		b.cancel()
	}
}

// Wait blocks until the broker consumer exits.
func (b *BrokerStreamConsumer) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *BrokerStreamConsumer) run(ctx context.Context) {
	if err := b.cli.Connect(); err != nil {
		slog.Error("broker consumer failed to connect", "group", b.cfg.Group, "error", err)
		return
	}
	defer func() { _ = b.cli.Close() }()

	start, err := b.cli.CommittedOffset(b.cfg.Topic, b.cfg.Partition, b.cfg.Group)
	if err != nil {
		slog.Error("broker consumer committed offset read failed", "group", b.cfg.Group, "error", err)
		return
	}

	batch := make([]*campaignmodel.Event, 0, b.cfg.BatchSize)
	lastFlush := time.Now()
	var batchCommit uint64
	var batchStartOffset uint64

	for {
		if ctx.Err() != nil {
			b.drain(ctx, batchCommit, batch)
			return
		}

		iter, err := b.cli.Fetch(b.cfg.Topic, b.cfg.Partition, start, b.cfg.MaxBytes)
		if err != nil {
			slog.Error("broker consumer fetch failed", "group", b.cfg.Group, "error", err)
			select {
			case <-ctx.Done():
				b.drain(ctx, batchCommit, batch)
				return
			case <-time.After(time.Second):
			}
			continue
		}

		var nextCommit uint64
		processed := 0
		for iter.Next() {
			if ctx.Err() != nil {
				break
			}
			evt, parseErr := ParseBrokerPayload(iter.Payload)
			if parseErr != nil {
				metrics.BrokerIngestParseErrorsTotal.WithLabelValues(b.cfg.Topic, b.cfg.Group).Inc()
				slog.Warn("broker payload parse failed", "group", b.cfg.Group, "offset", iter.Offset, "error", parseErr)
				nextCommit = iter.Offset + 1
				batchCommit = nextCommit
				continue
			}
			metrics.BrokerIngestMessagesTotal.WithLabelValues(b.cfg.Topic, b.cfg.Group, evt.Type).Inc()
			if len(batch) == 0 {
				batchStartOffset = iter.Offset
			}
			batch = append(batch, evt)
			nextCommit = iter.Offset + 1
			batchCommit = nextCommit
			processed++

			if len(batch) >= b.cfg.BatchSize || time.Since(lastFlush) >= b.cfg.FlushInt {
				committed, flushErr := b.flushAndCommit(ctx, batch, batchStartOffset, nextCommit)
				if flushErr != nil {
					return
				}
				start = committed
				batch = batch[:0]
				batchCommit = start
				lastFlush = time.Now()
			}
		}

		if nextCommit > 0 && len(batch) > 0 {
			committed, flushErr := b.flushAndCommit(ctx, batch, batchStartOffset, nextCommit)
			if flushErr != nil {
				return
			}
			start = committed
			batch = batch[:0]
			batchCommit = start
			lastFlush = time.Now()
		} else if nextCommit > start {
			stored, commitErr := b.cli.CommitOffset(b.cfg.Topic, b.cfg.Partition, b.cfg.Group, nextCommit)
			if commitErr != nil {
				slog.Error("broker offset commit failed", "group", b.cfg.Group, "error", commitErr)
				return
			}
			start = stored
			batchCommit = start
		}

		if ctx.Err() != nil {
			b.drain(ctx, batchCommit, batch)
			return
		}

		if processed == 0 {
			select {
			case <-ctx.Done():
				b.drain(ctx, batchCommit, batch)
				return
			case <-time.After(b.cfg.IdleWait):
			}
		}
	}
}

func (b *BrokerStreamConsumer) drain(ctx context.Context, nextCommit uint64, batch []*campaignmodel.Event) {
	if len(batch) == 0 || nextCommit == 0 {
		return
	}
	startOffset := nextCommit - uint64(len(batch))
	_, _ = b.flushAndCommit(ctx, batch, startOffset, nextCommit)
}

func (b *BrokerStreamConsumer) flushAndCommit(ctx context.Context, batch []*campaignmodel.Event, offsetStart, nextCommit uint64) (uint64, error) {
	if len(batch) == 0 {
		return nextCommit, nil
	}

	if !b.cb.Allow() {
		wait := b.cb.WaitDuration()
		if wait <= 0 {
			wait = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(wait):
		}
		return 0, errors.New("circuit breaker open")
	}

	if b.cfg.ShadowMode {
		metrics.BrokerShadowMessagesTotal.WithLabelValues(b.cfg.Topic, b.cfg.Group).Add(float64(len(batch)))
		for _, evt := range batch {
			b.writeAudit(evt)
			campaignmodel.EventPool.Put(evt)
		}
		b.cb.RecordSuccess(b.cfg.Group)
	} else {
		var claim dedup.ClaimResult
		if b.dedup != nil {
			clickIDs := make([]string, len(batch))
			for i, evt := range batch {
				clickIDs[i] = evt.ClickID
			}
			seqEnd := int64(nextCommit) - 1
			if seqEnd < int64(offsetStart) {
				seqEnd = int64(offsetStart)
			}
			scope := b.dedup.RegionScope(
				dedupkey.BrokerSourceID(b.cfg.Topic, b.cfg.Partition, b.cfg.Group),
				int64(offsetStart),
				seqEnd,
			)
			factorU := dedupkey.FactorU(dedupkey.CanonicalBrokerPayload(clickIDs))
			var claimErr error
			claim, claimErr = b.dedup.ClaimConfirm(ctx, scope, factorU)
			if claimErr != nil {
				return 0, claimErr
			}
			if guardErr := dedup.GuardOutcome(claim); guardErr != nil {
				return 0, guardErr
			}
			if claim.Outcome == dedup.OutcomeAlreadyConfirmed {
				resume, resumeErr := b.dedup.NeedsResumeApply(ctx, claim.DedupKey)
				if resumeErr != nil {
					return 0, resumeErr
				}
				if !resume {
					goto commitOffset
				}
			}
		}
		storeCtx, cancel := context.WithTimeout(ctx, b.writeTimeout)
		err := b.storeWithRetry(storeCtx, batch)
		cancel()
		if err != nil {
			b.cb.RecordFailure(b.cfg.Group)
			slog.Error("broker consumer store batch failed", "group", b.cfg.Group, "error", err)
			return 0, err
		}
		if claim.DedupKey != "" {
			_ = b.dedup.RecordApply(ctx, claim.DedupKey)
		}
		b.cb.RecordSuccess(b.cfg.Group)
		for _, evt := range batch {
			b.writeAudit(evt)
			campaignmodel.EventPool.Put(evt)
		}
	}

commitOffset:
	stored, err := b.cli.CommitOffset(b.cfg.Topic, b.cfg.Partition, b.cfg.Group, nextCommit)
	if err != nil {
		slog.Error("broker offset commit failed", "group", b.cfg.Group, "error", err)
		return 0, err
	}
	metrics.BrokerIngestCommitsTotal.WithLabelValues(b.cfg.Topic, b.cfg.Group).Inc()
	return stored, nil
}

func (b *BrokerStreamConsumer) storeWithRetry(ctx context.Context, batch []*campaignmodel.Event) error {
	wait := b.retryInit
	for attempt := 0; attempt <= b.maxRetries; attempt++ {
		err := b.store.StoreBatch(ctx, batch)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		if attempt == b.maxRetries {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		if wait < b.retryMax {
			wait *= 2
			if wait > b.retryMax {
				wait = b.retryMax
			}
		}
	}
	return errors.New("store retries exhausted")
}

func (b *BrokerStreamConsumer) writeAudit(evt *campaignmodel.Event) {
	if b.logger == nil || evt == nil {
		return
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	writeAuditLog(b.logger, &b.auditLogSeq, auditLogSampleMaskDefault, 0, evt)
}
