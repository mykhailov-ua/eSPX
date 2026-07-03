package rtbbridge

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"espx/internal/ads/catalog"
	"espx/internal/ads/sharding"
	"espx/internal/domain"
	"espx/internal/metrics"
	"espx/internal/rtb"

	redis "github.com/redis/go-redis/v9"
)

// RtbBudgetReconcileConfig tunes Redis versus RTB budget sampling.
type RtbBudgetReconcileConfig struct {
	Interval            time.Duration
	DivergenceThreshold int64
	SampleSize          int
}

// RtbBudgetReconcileWorker samples Redis campaign budgets against the in-process RTB store.
type RtbBudgetReconcileWorker struct {
	cfg      RtbBudgetReconcileConfig
	registry *catalog.Registry
	catalog  *RtbCatalog
	rdbs     []redis.UniversalClient
	sharder  sharding.Sharder
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewRtbBudgetReconcileWorker creates a cold-path budget divergence sampler.
func NewRtbBudgetReconcileWorker(
	cfg RtbBudgetReconcileConfig,
	registry *catalog.Registry,
	catalog *RtbCatalog,
	rdbs []redis.UniversalClient,
	sharder sharding.Sharder,
) *RtbBudgetReconcileWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.DivergenceThreshold <= 0 {
		cfg.DivergenceThreshold = 1000
	}
	if cfg.SampleSize <= 0 {
		cfg.SampleSize = 32
	}
	return &RtbBudgetReconcileWorker{
		cfg:      cfg,
		registry: registry,
		catalog:  catalog,
		rdbs:     rdbs,
		sharder:  sharder,
	}
}

// Start launches periodic budget reconcile sampling.
func (w *RtbBudgetReconcileWorker) Start(ctx context.Context) {
	if w == nil || w.registry == nil || w.catalog == nil || len(w.rdbs) == 0 || w.sharder == nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run(runCtx)
	}()
}

// Close stops the reconcile worker.
func (w *RtbBudgetReconcileWorker) Close() {
	if w != nil && w.cancel != nil {
		w.cancel()
	}
}

// Wait blocks until the worker exits.
func (w *RtbBudgetReconcileWorker) Wait(ctx context.Context) error {
	if w == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *RtbBudgetReconcileWorker) run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sample(ctx)
		}
	}
}

func (w *RtbBudgetReconcileWorker) sample(ctx context.Context) {
	sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	campaigns := w.registry.ActiveCampaigns()
	if len(campaigns) == 0 {
		metrics.RtbBudgetReconcileHigh.Set(0)
		return
	}

	store := w.catalog.Registry().Store()

	limit := w.cfg.SampleSize
	if limit > len(campaigns) {
		limit = len(campaigns)
	}

	var maxDelta int64
	for i := 0; i < limit; i++ {
		camp := campaigns[i]
		if camp == nil || camp.BudgetCampaignKey == "" {
			continue
		}
		redisRem, ok := loadRedisCampaignBudget(sampleCtx, w.rdbs, w.sharder, camp)
		if !ok {
			continue
		}
		rtbRem := store.GetBudget(CampaignIDFromUUID(camp.ID))
		delta := redisRem - rtbRem
		if delta < 0 {
			delta = -delta
		}
		metrics.RtbBudgetReconcileDivergenceMicro.Observe(float64(delta))
		metrics.RtbBudgetReconcileSamplesTotal.Inc()
		if delta > maxDelta {
			maxDelta = delta
		}
	}

	if maxDelta > w.cfg.DivergenceThreshold {
		metrics.RtbBudgetReconcileHigh.Set(1)
		slog.Warn("rtb budget reconcile divergence high", "max_delta_micro", maxDelta, "threshold", w.cfg.DivergenceThreshold)
	} else {
		metrics.RtbBudgetReconcileHigh.Set(0)
	}
}

// ReconcileCampaignBudget compares Redis and RTB remaining budget for one campaign.
func ReconcileCampaignBudget(
	ctx context.Context,
	store *rtb.BudgetStore,
	rdbs []redis.UniversalClient,
	sharder sharding.Sharder,
	camp *domain.Campaign,
) (redisRem int64, rtbRem int64, ok bool) {
	if store == nil || camp == nil || len(rdbs) == 0 || sharder == nil {
		return 0, 0, false
	}
	redisRem, ok = loadRedisCampaignBudget(ctx, rdbs, sharder, camp)
	if !ok {
		return 0, 0, false
	}
	rtbRem = store.GetBudget(CampaignIDFromUUID(camp.ID))
	return redisRem, rtbRem, true
}
