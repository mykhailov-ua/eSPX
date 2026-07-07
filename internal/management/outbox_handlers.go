package management

import (
	"context"
	"fmt"

	"espx/internal/ads/db"
	"espx/pkg/cold"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// campaignIDPayload carries a campaign identifier in lightweight outbox events.
type campaignIDPayload struct {
	CampaignID string `json:"campaign_id"`
}

// brandIDPayload carries a brand identifier in lightweight outbox events.
type brandIDPayload struct {
	BrandID string `json:"brand_id"`
}

// campaignPacingPayload carries pacing mode updates in outbox events.
type campaignPacingPayload struct {
	CampaignID string `json:"campaign_id"`
	PacingMode string `json:"pacing_mode"`
}

// handleOutboxEvent dispatches a claimed outbox row to the Redis side-effect handler for its type.
func (w *OutboxWorker) handleOutboxEvent(opCtx, ctx context.Context, ev db.OutboxEvent) error {
	switch ev.EventType {
	case "CREATE_CAMPAIGN":
		return w.handleCreateCampaign(ctx, ev.Payload)
	case "PAUSE_CAMPAIGN":
		return w.handlePauseCampaign(ctx, ev.Payload)
	case "RESUME_CAMPAIGN":
		return w.handleResumeCampaign(ctx, ev.Payload)
	case "UPDATE_CAMPAIGN_SCHEDULE":
		return w.handleUpdateCampaignSchedule(ctx, ev.Payload)
	case "SYNC_BRAND_CREATIVES":
		return w.handleSyncBrandCreatives(ctx, ev.Payload)
	case "CANCEL_CAMPAIGN":
		return w.handleCancelCampaign(ctx, ev.Payload)
	case "UPDATE_CAMPAIGN_PACING":
		return w.handleUpdateCampaignPacing(ctx, ev.Payload)
	case "UPDATE_CAMPAIGN_FRAUD":
		return w.handleUpdateCampaignFraud(ctx, ev.Payload)
	case "UPDATE_SETTINGS":
		return w.handleUpdateSettings(opCtx, ev.ID, ev.Payload)
	case "UPDATE_BLACKLIST":
		return w.handleUpdateBlacklist(ctx, ev.Payload)
	case "CONFIGURE_BRAND_FCAP":
		return w.handleConfigureBrandFcap(ctx, ev.Payload)
	case "UPDATE_SUPPLY_FILES":
		return w.handleUpdateSupplyFiles(ctx, ev.Payload)
	case "RELOAD_RTB_CATALOG":
		return w.handleReloadRtbCatalog(ctx, ev.Payload)
	default:
		return fmt.Errorf("unknown outbox event type: %s", ev.EventType)
	}
}

// handleCreateCampaign seeds Redis budget keys and publishes a campaign cache invalidation.
func (w *OutboxWorker) handleCreateCampaign(ctx context.Context, payload []byte) error {
	p, err := cold.UnmarshalStrict[CampaignPayload](payload)
	if err != nil {
		return err
	}
	campUUID, err := uuid.Parse(p.CampaignID)
	if err != nil {
		return fmt.Errorf("invalid campaign id in payload: %w", err)
	}
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return fmt.Errorf("no redis client available")
	}
	_, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		return w.setCampaignBudgetRemaining(ctx, pipe, p.CampaignID, campUUID, p.BudgetLimit)
	})
	if err != nil {
		return err
	}
	return w.svc.publishCampaignUpdate(ctx, p.CampaignID)
}

// handlePauseCampaign removes Redis budget keys when delivery stops.
func (w *OutboxWorker) handlePauseCampaign(ctx context.Context, payload []byte) error {
	p := cold.UnmarshalLenient[CampaignPayload](payload)
	if p.CampaignID == "" {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	return w.deleteCampaignBudgetAndPublish(ctx, p.CampaignID, campUUID)
}

// handleResumeCampaign restores Redis budget keys when delivery resumes.
func (w *OutboxWorker) handleResumeCampaign(ctx context.Context, payload []byte) error {
	p := cold.UnmarshalLenient[CampaignPayload](payload)
	if p.CampaignID == "" {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	return w.setCampaignBudgetAndPublish(ctx, p, campUUID)
}

// handleUpdateCampaignSchedule notifies the hot path that schedule metadata changed.
func (w *OutboxWorker) handleUpdateCampaignSchedule(ctx context.Context, payload []byte) error {
	p := cold.UnmarshalLenient[campaignIDPayload](payload)
	if p.CampaignID == "" {
		return nil
	}
	if _, err := uuid.Parse(p.CampaignID); err != nil {
		return nil
	}
	return w.svc.publishCampaignUpdate(ctx, p.CampaignID)
}

// handleUpdateCampaignFraud notifies trackers that fraud thresholds or behavior flags changed.
func (w *OutboxWorker) handleUpdateCampaignFraud(ctx context.Context, payload []byte) error {
	p := cold.UnmarshalLenient[campaignIDPayload](payload)
	if p.CampaignID == "" {
		return nil
	}
	if _, err := uuid.Parse(p.CampaignID); err != nil {
		return nil
	}
	return w.svc.publishCampaignUpdate(ctx, p.CampaignID)
}

// handleSyncBrandCreatives refreshes weighted landing URLs in Redis for a brand.
func (w *OutboxWorker) handleSyncBrandCreatives(ctx context.Context, payload []byte) error {
	p := cold.UnmarshalLenient[brandIDPayload](payload)
	if p.BrandID == "" {
		return nil
	}
	return w.syncBrandCreativesToRedis(ctx, p.BrandID)
}

// handleCancelCampaign clears Redis budget state when a campaign enters draining cancellation.
func (w *OutboxWorker) handleCancelCampaign(ctx context.Context, payload []byte) error {
	p := cold.UnmarshalLenient[CampaignPayload](payload)
	if p.CampaignID == "" {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	return w.deleteCampaignBudgetAndPublish(ctx, p.CampaignID, campUUID)
}

// handleUpdateCampaignPacing writes pacing mode to Redis and invalidates campaign caches.
func (w *OutboxWorker) handleUpdateCampaignPacing(ctx context.Context, payload []byte) error {
	p := cold.UnmarshalLenient[campaignPacingPayload](payload)
	if p.CampaignID == "" {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return nil
	}
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, fmt.Sprintf("campaign:settings:%s", p.CampaignID), "pacing_mode", p.PacingMode)
		return nil
	})
	if err != nil {
		return err
	}
	return w.svc.publishCampaignUpdate(ctx, p.CampaignID)
}

// handleUpdateSettings pushes system settings and a monotonic version to Redis config keys.
func (w *OutboxWorker) handleUpdateSettings(opCtx context.Context, eventID int64, payload []byte) error {
	p, err := cold.UnmarshalStrict[SettingsPayload](payload)
	if err != nil {
		return err
	}
	if len(w.svc.rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	return syncGlobalConfigToAllShards(opCtx, w.svc.rdbs, p.Settings, eventID)
}

// handleUpdateBlacklist applies an IP block or unblock to every Redis shard.
func (w *OutboxWorker) handleUpdateBlacklist(ctx context.Context, payload []byte) error {
	p, err := cold.UnmarshalStrict[BlacklistPayload](payload)
	if err != nil {
		return err
	}
	return w.applyBlacklistPayload(ctx, p)
}

// handleConfigureBrandFcap invalidates active campaigns when brand frequency caps change.
func (w *OutboxWorker) handleConfigureBrandFcap(ctx context.Context, payload []byte) error {
	p, err := cold.UnmarshalStrict[brandIDPayload](payload)
	if err != nil {
		return err
	}
	brandUUID, err := uuid.Parse(p.BrandID)
	if err != nil {
		return err
	}
	campIDs, err := w.listActiveCampaignIDsByBrand(ctx, brandUUID)
	if err != nil {
		return err
	}
	if len(campIDs) == 0 {
		return nil
	}
	rdb := w.svc.getPubSubRDB()
	if rdb == nil {
		return nil
	}
	channel := w.svc.campaignUpdateChannel()
	_, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, cidStr := range campIDs {
			pipe.Publish(ctx, channel, cidStr)
		}
		return nil
	})
	return err
}

// listActiveCampaignIDsByBrand finds campaigns that must reload brand fcap settings from Redis pubsub.
func (w *OutboxWorker) listActiveCampaignIDsByBrand(ctx context.Context, brandUUID uuid.UUID) ([]string, error) {
	rows, err := w.svc.GetPool().Query(ctx, "SELECT id FROM campaigns WHERE brand_id = $1 AND status = 'ACTIVE'", ToUUID(brandUUID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var campIDs []string
	for rows.Next() {
		var cid uuid.UUID
		if scanErr := rows.Scan(&cid); scanErr == nil {
			campIDs = append(campIDs, cid.String())
		}
	}
	return campIDs, nil
}

// setCampaignBudgetAndPublish restores budget keys and notifies the hot path on resume or create.
func (w *OutboxWorker) setCampaignBudgetAndPublish(ctx context.Context, p CampaignPayload, campUUID uuid.UUID) error {
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return nil
	}
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		return w.setCampaignBudgetRemaining(ctx, pipe, p.CampaignID, campUUID, p.BudgetLimit)
	})
	if err != nil {
		return err
	}
	return w.svc.publishCampaignUpdate(ctx, p.CampaignID)
}

// deleteCampaignBudgetAndPublish removes budget keys and notifies the hot path on pause or cancel.
func (w *OutboxWorker) deleteCampaignBudgetAndPublish(ctx context.Context, campaignIDStr string, campUUID uuid.UUID) error {
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return nil
	}
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, fmt.Sprintf("budget:campaign:%s", campaignIDStr))
		return nil
	})
	if err != nil {
		return err
	}
	return w.svc.publishCampaignUpdate(ctx, campaignIDStr)
}
