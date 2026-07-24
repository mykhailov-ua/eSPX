package ingestion

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	// SlotMigrationDualWriteFlagKey marks a source shard as dual-writing debits to the delta stream.
	SlotMigrationDualWriteFlagKey = "slot_migration:dual_write"
	// SlotMigrationDeltaStreamKey holds debit deltas replicated from source during hot-slot migration.
	SlotMigrationDeltaStreamKey = "slot_migration:delta"
	// SlotMigrationDeltaCursorKey stores the last applied stream entry ID for catch-up.
	SlotMigrationDeltaCursorKey = "slot_migration:delta:cursor"
	slotMigrationDualWriteTTL   = 24 * time.Hour
	slotMigrationDeltaMaxLen    = 100_000
)

// SlotMigrationDualWriteConfig tunes lag gates for zero-downtime cutover (M1-08).
type SlotMigrationDualWriteConfig struct {
	Enabled      bool
	LagEpsilon   int64
	LagThreshold int64
}

// EnableSlotMigrationDualWrite arms source-shard Lua to XADD debit deltas without fencing debits.
func EnableSlotMigrationDualWrite(
	ctx context.Context,
	src redis.Cmdable,
	version int32,
	slot, targetShard int16,
) error {
	if src == nil {
		return fmt.Errorf("nil redis source")
	}
	meta := fmt.Sprintf("%d:%d:%d", version, slot, targetShard)
	pipe := src.Pipeline()
	pipe.Set(ctx, SlotMigrationDualWriteFlagKey, meta, slotMigrationDualWriteTTL)
	pipe.Del(ctx, SlotMigrationDeltaCursorKey)
	_, err := pipe.Exec(ctx)
	return err
}

// DisableSlotMigrationDualWrite clears dual-write mode and delta stream state on the source shard.
func DisableSlotMigrationDualWrite(ctx context.Context, src redis.Cmdable) error {
	if src == nil {
		return nil
	}
	pipe := src.Pipeline()
	pipe.Del(ctx, SlotMigrationDualWriteFlagKey)
	pipe.Del(ctx, SlotMigrationDeltaStreamKey)
	pipe.Del(ctx, SlotMigrationDeltaCursorKey)
	_, err := pipe.Exec(ctx)
	return err
}

// SlotMigrationDelta is one debit replicated from source to target during dual-write catch-up.
type SlotMigrationDelta struct {
	CampaignID uuid.UUID
	Amount     int64
	SpendKey   string
}

// CatchUpSlotMigrationDeltas applies pending stream entries to the target shard and advances the cursor.
func CatchUpSlotMigrationDeltas(
	ctx context.Context,
	src, dst redis.Cmdable,
	version int32,
	slot int16,
) (applied int, lag int64, err error) {
	if src == nil || dst == nil {
		return 0, 0, fmt.Errorf("nil redis client")
	}

	cursor, err := src.Get(ctx, SlotMigrationDeltaCursorKey).Result()
	if err == redis.Nil {
		cursor = "0-0"
	} else if err != nil {
		return 0, 0, fmt.Errorf("read delta cursor: %w", err)
	}

	entries, err := src.XRange(ctx, SlotMigrationDeltaStreamKey, openStreamRangeID(cursor), "+").Result()
	if err != nil {
		return 0, 0, fmt.Errorf("xrange delta stream: %w", err)
	}

	slotLabel := strconv.Itoa(int(slot))
	versionLabel := strconv.Itoa(int(version))

	for _, entry := range entries {
		delta, parseErr := parseSlotMigrationDelta(entry.Values)
		if parseErr != nil {
			metrics.SlotMigrationDualWriteTotal.WithLabelValues(slotLabel, "parse_error").Inc()
			return applied, int64(len(entries) - applied), fmt.Errorf("parse delta %s: %w", entry.ID, parseErr)
		}
		if applyErr := applySlotMigrationDelta(ctx, dst, delta); applyErr != nil {
			metrics.SlotMigrationDualWriteTotal.WithLabelValues(slotLabel, "apply_error").Inc()
			return applied, int64(len(entries) - applied), applyErr
		}
		cursor = entry.ID
		applied++
		metrics.SlotMigrationDualWriteTotal.WithLabelValues(slotLabel, "applied").Inc()
	}

	if applied > 0 {
		if err := src.Set(ctx, SlotMigrationDeltaCursorKey, cursor, slotMigrationDualWriteTTL).Err(); err != nil {
			return applied, 0, fmt.Errorf("advance delta cursor: %w", err)
		}
	}

	lag, err = SlotMigrationReplicationLag(ctx, src)
	if err != nil {
		return applied, 0, err
	}
	metrics.SlotMigrationLagMessages.WithLabelValues(slotLabel, versionLabel).Set(float64(lag))
	return applied, lag, nil
}

// SlotMigrationReplicationLag returns unconsumed delta stream entries after the catch-up cursor.
func SlotMigrationReplicationLag(ctx context.Context, src redis.Cmdable) (int64, error) {
	if src == nil {
		return 0, fmt.Errorf("nil redis source")
	}
	cursor, err := src.Get(ctx, SlotMigrationDeltaCursorKey).Result()
	if err == redis.Nil {
		cursor = "0-0"
	} else if err != nil {
		return 0, err
	}
	entries, err := src.XRange(ctx, SlotMigrationDeltaStreamKey, openStreamRangeID(cursor), "+").Result()
	if err != nil {
		return 0, err
	}
	return int64(len(entries)), nil
}

func openStreamRangeID(cursor string) string {
	if cursor == "" || cursor == "0-0" {
		return "-"
	}
	return "(" + cursor
}

func parseSlotMigrationDelta(values map[string]interface{}) (SlotMigrationDelta, error) {
	var delta SlotMigrationDelta
	campRaw, ok := values["campaign_id"]
	if !ok {
		return delta, fmt.Errorf("missing campaign_id")
	}
	campStr, ok := campRaw.(string)
	if !ok {
		return delta, fmt.Errorf("campaign_id not string")
	}
	id, err := uuid.Parse(campStr)
	if err != nil {
		return delta, err
	}
	delta.CampaignID = id

	amountRaw, ok := values["amount"]
	if !ok {
		return delta, fmt.Errorf("missing amount")
	}
	switch v := amountRaw.(type) {
	case string:
		delta.Amount, err = strconv.ParseInt(v, 10, 64)
	case int64:
		delta.Amount = v
	default:
		return delta, fmt.Errorf("amount type %T", amountRaw)
	}
	if err != nil {
		return delta, err
	}

	spendRaw, ok := values["spend_key"]
	if !ok {
		delta.SpendKey = budgetCampaignKey(id)
		return delta, nil
	}
	spendStr, ok := spendRaw.(string)
	if !ok {
		return delta, fmt.Errorf("spend_key not string")
	}
	delta.SpendKey = spendStr
	return delta, nil
}

func applySlotMigrationDelta(ctx context.Context, dst redis.Cmdable, delta SlotMigrationDelta) error {
	if delta.Amount == 0 {
		return nil
	}
	exists, err := dst.Exists(ctx, delta.SpendKey).Result()
	if err != nil {
		return fmt.Errorf("exists spend key: %w", err)
	}
	if exists == 0 {
		return fmt.Errorf("target missing spend key %q for campaign %s", delta.SpendKey, delta.CampaignID)
	}
	pipe := dst.Pipeline()
	pipe.IncrBy(ctx, delta.SpendKey, -delta.Amount)
	pipe.IncrBy(ctx, campaignSyncKey(delta.CampaignID), delta.Amount)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("apply delta campaign %s: %w", delta.CampaignID, err)
	}
	return nil
}

// PublishSlotMigrationDeltaTestHelper enqueues one delta entry (tests only).
func PublishSlotMigrationDeltaTestHelper(ctx context.Context, src redis.Cmdable, delta SlotMigrationDelta) error {
	if src == nil {
		return fmt.Errorf("nil redis source")
	}
	return src.XAdd(ctx, &redis.XAddArgs{
		Stream: SlotMigrationDeltaStreamKey,
		MaxLen: slotMigrationDeltaMaxLen,
		Approx: true,
		Values: map[string]interface{}{
			"campaign_id": delta.CampaignID.String(),
			"amount":      strconv.FormatInt(delta.Amount, 10),
			"spend_key":   delta.SpendKey,
		},
	}).Err()
}
