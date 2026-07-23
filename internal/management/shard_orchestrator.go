package management

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
)

// PaddedEma pads the float64 counter to prevent false sharing on CPU cache lines.
type PaddedEma struct {
	Value float64
	_     [56]byte // padding to align to 64-byte cache line boundary
}

// ShardOrchestrator monitors Redis shard capacity and triggers automated Triplet migrations.
type ShardOrchestrator struct {
	svc             *Service
	metricsProvider ShardMetricsProvider
	interval        time.Duration
	cooldown        time.Duration
	scaleThreshold  float64
	overloadLimit   time.Duration

	mu            sync.Mutex
	lastScaleTime time.Time
	overloadStart map[int16]time.Time
	shardEma      map[int16]*PaddedEma
	campaignEma   map[uuid.UUID]*PaddedEma
}

// NewShardOrchestrator constructs a ShardOrchestrator with 64-byte padded EWMA fields.
func NewShardOrchestrator(svc *Service, provider ShardMetricsProvider, interval time.Duration) *ShardOrchestrator {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &ShardOrchestrator{
		svc:             svc,
		metricsProvider: provider,
		interval:        interval,
		cooldown:        3600 * time.Second,
		scaleThreshold:  0.85,
		overloadLimit:   300 * time.Second,
		overloadStart:   make(map[int16]time.Time),
		shardEma:        make(map[int16]*PaddedEma),
		campaignEma:     make(map[uuid.UUID]*PaddedEma),
	}
}

// Start runs the orchestration loop until ctx is cancelled.
func (o *ShardOrchestrator) Start(ctx context.Context) {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.tick(ctx)
		}
	}
}

func (o *ShardOrchestrator) tick(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()

	numShards := int16(len(o.svc.rdbs))
	if numShards <= 1 {
		return
	}

	// 1. Collect metrics and update shard EWMA
	alpha := 0.15 // ~60s window with 10s interval
	var maxShard int16 = -1
	var maxEma float64 = -1.0

	for i := int16(0); i < numShards; i++ {
		m, err := o.metricsProvider.GetMetrics(ctx, i, o.svc.rdbs[i])
		if err != nil {
			slog.Warn("orchestrator: failed to get metrics", "shard", i, "error", err)
			continue
		}

		// Calculate raw capacity score (0.0 - 1.0)
		cpuScore := m.CPUUsage / 100.0
		memScore := m.MemoryPct / 100.0
		opsScore := 0.0
		if m.OpsPerSec > 0 {
			opsScore = float64(m.OpsPerSec) / 50000.0 // assume 50k ops/sec limit
		}

		rawScore := cpuScore
		if memScore > rawScore {
			rawScore = memScore
		}
		if opsScore > rawScore {
			rawScore = opsScore
		}

		ema, ok := o.shardEma[i]
		if !ok {
			ema = &PaddedEma{Value: rawScore}
			o.shardEma[i] = ema
		} else {
			ema.Value = alpha*rawScore + (1.0-alpha)*ema.Value
		}

		if ema.Value > maxEma {
			maxEma = ema.Value
			maxShard = i
		}
	}

	// 2. Check overload duration and trigger migration
	if maxShard != -1 && maxEma >= o.scaleThreshold {
		start, ok := o.overloadStart[maxShard]
		if !ok {
			o.overloadStart[maxShard] = time.Now()
			slog.Info("orchestrator: shard capacity threshold exceeded", "shard", maxShard, "ema", maxEma)
		} else if time.Since(start) >= o.overloadLimit {
			// Quorum gate check & cooldown check
			if time.Since(o.lastScaleTime) >= o.cooldown {
				slog.Info("orchestrator: triggering scale-out migration", "shard", maxShard, "ema", maxEma)
				if err := o.migrateLoad(ctx, maxShard); err == nil {
					o.lastScaleTime = time.Now()
					delete(o.overloadStart, maxShard)
				} else {
					slog.Error("orchestrator: migration failed", "shard", maxShard, "error", err)
				}
			} else {
				slog.Debug("orchestrator: migration skipped due to cooldown", "shard", maxShard)
			}
		}
	} else if maxShard != -1 {
		delete(o.overloadStart, maxShard)
	}
}

func (o *ShardOrchestrator) migrateLoad(ctx context.Context, sourceShard int16) error {
	// Find the campaign with the highest load on this shard
	campaigns, err := o.svc.listActiveCampaignUUIDs(ctx)
	if err != nil {
		return err
	}

	sharder := ingestion.NewStaticSlotSharder(len(o.svc.rdbs))
	var bestCampaign uuid.UUID
	var maxCampaignLoad float64 = -1.0

	for _, id := range campaigns {
		if int16(sharder.GetShard(id)) == sourceShard {
			// In a real system, we would query metrics. Here we use a mock/random load or EWMA
			load := 0.5 // default
			ema, ok := o.campaignEma[id]
			if !ok {
				ema = &PaddedEma{Value: load}
				o.campaignEma[id] = ema
			}
			if ema.Value > maxCampaignLoad {
				maxCampaignLoad = ema.Value
				bestCampaign = id
			}
		}
	}

	if bestCampaign == uuid.Nil {
		return fmt.Errorf("no campaign found on overloaded shard %d", sourceShard)
	}

	// Find the least loaded target shard
	var targetShard int16 = -1
	var minEma float64 = 1e18
	for i := int16(0); i < int16(len(o.svc.rdbs)); i++ {
		if i == sourceShard {
			continue
		}
		ema, ok := o.shardEma[i]
		if ok && ema.Value < minEma {
			minEma = ema.Value
			targetShard = i
		}
	}

	if targetShard == -1 {
		return fmt.Errorf("no target shard found for migration")
	}

	slog.Info("orchestrator: initiating campaign migration", "campaign", bestCampaign, "source", sourceShard, "target", targetShard)

	// Perform Triplet migration:
	// 1. Update PG assignment and increment migration_gen
	tx, err := o.svc.GetPool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := db.New(tx)
	_, err = q.UpsertCampaignShardAssignment(ctx, db.UpsertCampaignShardAssignmentParams{
		CampaignID:    ingestion.ToUUID(bestCampaign),
		PrimaryAShard: targetShard, // migrate to target
		PrimaryBShard: targetShard,
		ReserveShard:  targetShard,
		HEma:          0.5,
		CEma:          0.5,
	})
	if err != nil {
		return err
	}

	// Increment migration_gen on campaign
	row, err := q.GetCampaignForUpdate(ctx, ingestion.ToUUID(bestCampaign))
	if err != nil {
		return err
	}
	_, err = q.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
		ID:     ingestion.ToUUID(bestCampaign),
		Status: row.Status, // keep status, but trigger update
	})
	if err != nil {
		return err
	}

	// Bump migration_gen in DB
	_, err = tx.Exec(ctx, "UPDATE campaigns SET migration_gen = migration_gen + 1 WHERE id = $1", ingestion.ToUUID(bestCampaign))
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// 2. Set migration fence on source shard
	srcRdb := o.svc.rdbs[sourceShard]
	dstRdb := o.svc.rdbs[targetShard]
	if err := ingestion.BumpMigrationFences(ctx, o.svc.GetPool(), srcRdb, []uuid.UUID{bestCampaign}); err != nil {
		return err
	}

	// 3. Copy campaign keys from source to target
	migrator := &ingestion.CampaignKeyMigrator{}
	if _, err := migrator.MigrateCampaignKeys(ctx, srcRdb, dstRdb, bestCampaign); err != nil {
		return err
	}

	// 4. Drain old keys from source shard
	if _, err := migrator.DrainCampaignKeys(ctx, srcRdb, bestCampaign); err != nil {
		return err
	}

	slog.Info("orchestrator: campaign migration completed successfully", "campaign", bestCampaign)
	return nil
}
