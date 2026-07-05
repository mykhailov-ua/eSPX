package logcompactor

import (
	"bytes"
	"context"
	"os"
	"strings"
	"time"
)

// ColdConfig configures warm-to-ClickHouse cold-tier rollup.
type ColdConfig struct {
	WarmMinAge            time.Duration
	WorkInterval          time.Duration
	WarmDir               string
	DeleteWarmAfterRollup bool
}

// ColdRolluper aggregates warm segments into ClickHouse audit_log_rollups.
type ColdRolluper struct {
	cfg        ColdConfig
	store      *LocalTierStore
	checkpoint *CheckpointStore
	inserter   RollupInserter
}

// NewColdRolluper wires warm listing, checkpointing, and rollup insert.
func NewColdRolluper(cfg ColdConfig, store *LocalTierStore, checkpoint *CheckpointStore, inserter RollupInserter) *ColdRolluper {
	if cfg.WorkInterval <= 0 {
		cfg.WorkInterval = 24 * time.Hour
	}
	if cfg.WarmDir == "" {
		cfg.WarmDir = store.WarmDir
	}
	return &ColdRolluper{
		cfg:        cfg,
		store:      store,
		checkpoint: checkpoint,
		inserter:   inserter,
	}
}

// Run executes cold rollup passes until ctx is cancelled.
func (cr *ColdRolluper) Run(ctx context.Context) error {
	if err := cr.checkpoint.Load(); err != nil {
		return err
	}
	if err := cr.RunOnce(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(cr.cfg.WorkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := cr.RunOnce(ctx); err != nil {
				return err
			}
		}
	}
}

// RunOnce processes all eligible warm segments once.
func (cr *ColdRolluper) RunOnce(ctx context.Context) error {
	cutoff := time.Now().Add(-cr.cfg.WarmMinAge)
	objects, err := cr.store.ListWarm(ctx, cutoff)
	if err != nil {
		coldListErrors.Inc()
		return err
	}

	refreshColdLag(objects, cr.checkpoint)

	var failed int
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := cr.rollupOne(ctx, obj); err != nil {
			coldRollupErrors.Inc()
			failed++
			continue
		}
	}
	if failed > 0 {
		return ErrColdRollupFailures
	}
	return nil
}

func (cr *ColdRolluper) rollupOne(ctx context.Context, obj TierObject) error {
	digest, err := computeFileDigest(obj.Path)
	if err != nil {
		return err
	}
	if cr.checkpoint.IsCompacted(obj.Key, digest.SHA256) {
		return nil
	}

	plain, err := ReadWarm(obj.Path)
	if err != nil {
		return err
	}

	rows, err := aggregateWarmSegment(bytes.NewReader(plain), obj.Key, digest.SHA256)
	if err != nil {
		return err
	}

	if err := cr.inserter.InsertRollups(ctx, rows); err != nil {
		return err
	}

	record := CheckpointRecord{
		SourceKey:     obj.Key,
		DestKey:       obj.Key,
		SourceSHA256:  digest.SHA256,
		DestSHA256:    digest.SHA256,
		OriginalCount: int64(len(rows)),
		KeptCount:     int64(len(rows)),
		CompactedAt:   time.Now().UTC(),
	}
	if err := cr.checkpoint.Save(record); err != nil {
		return err
	}

	coldRollupsTotal.Inc()
	coldRollupRowsTotal.Add(float64(len(rows)))

	if cr.cfg.DeleteWarmAfterRollup {
		_ = os.Remove(obj.Path)
		metaPath := strings.TrimSuffix(obj.Path, ".zst") + ".meta.json"
		_ = os.Remove(metaPath)
	}
	return nil
}
