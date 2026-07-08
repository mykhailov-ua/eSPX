package ivtdetector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DetectorConfig tunes scan cadence and management outbox backpressure.
type DetectorConfig struct {
	ScanInterval       time.Duration
	OutboxPendingLimit int64
	ManagementTimeout  time.Duration
	Analyzer           AnalyzerConfig
}

// DefaultDetectorConfig returns scan and backpressure defaults for production.
func DefaultDetectorConfig() DetectorConfig {
	return DetectorConfig{
		ScanInterval:       5 * time.Minute,
		OutboxPendingLimit: 500,
		ManagementTimeout:  10 * time.Second,
		Analyzer:           DefaultAnalyzerConfig(),
	}
}

// RunResult summarizes one detector cycle.
type RunResult struct {
	Candidates int
	Enqueued   int
	Skipped    int
	Backlogged bool
}

type suspiciousFinder = SuspiciousFinder

// Detector orchestrates ClickHouse analysis, idempotency claims, and management blacklist enqueue.
type Detector struct {
	analyzer   suspiciousFinder
	idem       *IdempotencyStore
	management BlacklistBlocker
	pool       *pgxpool.Pool
	cfg        DetectorConfig
}

// NewDetector wires analyzer, idempotency, management client, and Postgres for outbox pressure checks.
func NewDetector(
	analyzer suspiciousFinder,
	idem *IdempotencyStore,
	management BlacklistBlocker,
	pool *pgxpool.Pool,
	cfg DetectorConfig,
) *Detector {
	return &Detector{
		analyzer:   analyzer,
		idem:       idem,
		management: management,
		pool:       pool,
		cfg:        cfg,
	}
}

// Run executes one analysis cycle and enqueues blacklist:fraud updates for new suspicious IPs.
func (detector *Detector) Run(ctx context.Context) (RunResult, error) {
	var result RunResult
	if detector == nil {
		return result, fmt.Errorf("detector: nil receiver")
	}

	backlogged, err := detector.outboxBacklogged(ctx)
	if err != nil {
		return result, err
	}
	if backlogged {
		result.Backlogged = true
		ivtBackpressureDropsTotal.Inc()
		return result, ErrOutboxBackpressure
	}

	candidates, err := detector.analyzer.FindSuspiciousIPs(ctx)
	if err != nil {
		return result, err
	}
	result.Candidates = len(candidates)

	for _, candidate := range candidates {
		claimed, claimErr := detector.idem.TryClaim(ctx, candidate.IP)
		if claimErr != nil {
			return result, claimErr
		}
		if !claimed {
			result.Skipped++
			continue
		}

		blockErr := detector.management.BlockIP(ctx, candidate.IP)
		if blockErr != nil {
			if releaseErr := detector.idem.Release(ctx, candidate.IP); releaseErr != nil {
				slog.Error("failed to release idempotency claim after management error",
					"ip", candidate.IP,
					"block_error", blockErr,
					"release_error", releaseErr,
				)
			}
			return result, blockErr
		}

		result.Enqueued++
		ivtEnqueuedTotal.Inc()
		slog.Info("ivt detector enqueued fraud blacklist",
			"ip", candidate.IP,
			"signal", candidate.Reason,
			"score", candidate.Score,
		)
	}

	return result, nil
}

// RunLoop periodically executes detector cycles until the context is cancelled.
func (detector *Detector) RunLoop(ctx context.Context) error {
	if detector == nil {
		return fmt.Errorf("detector: nil receiver")
	}

	interval := detector.cfg.ScanInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if _, err := detector.Run(ctx); err != nil && err != ErrOutboxBackpressure && ctx.Err() == nil {
		slog.Error("ivt detector initial cycle failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			result, err := detector.Run(ctx)
			if err == ErrOutboxBackpressure {
				slog.Warn("ivt detector paused for outbox backpressure",
					"candidates", result.Candidates,
					"pending_limit", detector.cfg.OutboxPendingLimit,
				)
				continue
			}
			if err != nil && ctx.Err() == nil {
				slog.Error("ivt detector cycle failed", "error", err)
				continue
			}
			if result.Enqueued > 0 || result.Candidates > 0 {
				slog.Info("ivt detector cycle complete",
					"candidates", result.Candidates,
					"enqueued", result.Enqueued,
					"skipped", result.Skipped,
				)
			}
		}
	}
}

func (detector *Detector) outboxBacklogged(ctx context.Context) (bool, error) {
	if detector.pool == nil || detector.cfg.OutboxPendingLimit <= 0 {
		return false, nil
	}

	var pending int64
	err := detector.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'",
	).Scan(&pending)
	if err != nil {
		return false, fmt.Errorf("count pending outbox events: %w", err)
	}
	return pending >= detector.cfg.OutboxPendingLimit, nil
}

// PendingOutboxCount exposes the current PENDING outbox depth for tests and metrics hooks.
func (detector *Detector) PendingOutboxCount(ctx context.Context) (int64, error) {
	if detector.pool == nil {
		return 0, fmt.Errorf("detector: nil pool")
	}
	var pending int64
	err := detector.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'",
	).Scan(&pending)
	if err != nil {
		return 0, fmt.Errorf("count pending outbox events: %w", err)
	}
	return pending, nil
}
