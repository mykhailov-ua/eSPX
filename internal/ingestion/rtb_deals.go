package ingestion

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/config"
	"espx/internal/ingestion/sqlc"
	"espx/internal/rtb"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const defaultRtbCatalogReloadChannel = "rtb:catalog:reload"

func rtbDealRowToData(row db.RtbDeal) rtb.DealData {
	return rtb.DealData{
		DealID:     row.DealID,
		FloorMicro: row.FloorMicro,
		GeoMask:    uint64(row.GeoMask),
		CatMask:    uint64(row.CatMask),
		PacingOpen: rtb.DealPacingOpen(row.Pacing),
		Seats:      row.Seats,
		CustomerID: CustomerIDFromCustomerUUID(uuid.UUID(row.CustomerID.Bytes)),
	}
}

// ReloadRtbDeals loads all deals from Postgres and rebuilds the in-memory deal index.
func ReloadRtbDeals(ctx context.Context, q *db.Queries, catalog *RtbCatalog) error {
	if catalog == nil {
		return nil
	}
	rows, err := q.ListRtbDeals(ctx)
	if err != nil {
		return err
	}
	deals := make([]rtb.DealData, 0, len(rows))
	for _, row := range rows {
		deals = append(deals, rtbDealRowToData(row))
	}
	catalog.UpdateDeals(deals)
	return nil
}

// ReloadRtbCatalog reloads deals and rebuilds the campaign auction catalog.
func ReloadRtbCatalog(
	ctx context.Context,
	q *db.Queries,
	registry *Registry,
	catalog *RtbCatalog,
	cfg *config.Config,
	hybrid *HybridBalancer,
	budgetSync RtbBudgetSync,
) error {
	if err := ReloadRtbDeals(ctx, q, catalog); err != nil {
		return err
	}
	if registry != nil && catalog != nil && cfg != nil && cfg.RtbEnabled() {
		SyncRtbCatalog(ctx, registry, catalog, cfg, hybrid, budgetSync)
	}
	return nil
}

// StartRtbCatalogReloadWatch subscribes to Redis and rebuilds deals plus campaign catalog on reload signals.
func StartRtbCatalogReloadWatch(
	ctx context.Context,
	q *db.Queries,
	rdb redis.UniversalClient,
	channel string,
	registry *Registry,
	catalog *RtbCatalog,
	cfg *config.Config,
	hybrid *HybridBalancer,
	budgetSync RtbBudgetSync,
) {
	if rdb == nil || catalog == nil || q == nil {
		return
	}
	if channel == "" {
		channel = defaultRtbCatalogReloadChannel
	}

	reload := func() {
		reloadCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := ReloadRtbCatalog(reloadCtx, q, registry, catalog, cfg, hybrid, budgetSync); err != nil {
			slog.Error("rtb catalog reload failed", "error", err)
			return
		}
		slog.Info("rtb catalog reloaded via pubsub", "deals", catalog.DealCount())
	}

	go func() {
		pubsub := rdb.Subscribe(ctx, channel)
		defer pubsub.Close()

		ch := pubsub.Channel(redis.WithChannelSize(64))
		trigger := make(chan struct{}, 1)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-trigger:
					reload()
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					slog.Error("rtb catalog reload pubsub channel closed")
					return
				}
				if msg == nil {
					continue
				}
				select {
				case trigger <- struct{}{}:
				default:
				}
			}
		}
	}()
}

// RtbCatalogReloadChannel returns the Redis pubsub channel for deal catalog reload.
func RtbCatalogReloadChannel(cfg *config.Config) string {
	if cfg != nil && cfg.RtbCatalogReloadChannel != "" {
		return cfg.RtbCatalogReloadChannel
	}
	return defaultRtbCatalogReloadChannel
}
