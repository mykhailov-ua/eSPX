package ingestion

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// CampaignRedisKeyCatalog is the single source for slot-migration COPY/DRAIN key lists.
// Used by CampaignKeyMigrator, PG re-warm, and REDIS.md documentation.
type CampaignRedisKeyCatalog struct{}

// DefaultCampaignRedisKeyCatalog is the process-wide catalog instance.
var DefaultCampaignRedisKeyCatalog = NewCampaignRedisKeyCatalog()

// NewCampaignRedisKeyCatalog constructs a catalog.
func NewCampaignRedisKeyCatalog() *CampaignRedisKeyCatalog {
	return &CampaignRedisKeyCatalog{}
}

// FixedKeys returns exact Redis keys to COPY/DRAIN for one campaign.
// Migration fence keys are source-only and omitted from COPY (see SourceOnlyKeys).
func (c *CampaignRedisKeyCatalog) FixedKeys(id uuid.UUID) []string {
	idStr := id.String()
	tag := campaignHashTag(id)
	return []string{
		budgetCampaignKey(id),
		tag + "budget:quota:" + idStr,
		tag + "budget:refill_lock:" + idStr,
		BudgetFrozenRedisKey(id),
		campaignSyncKey(id),
		"budget:inflight:campaign:" + idStr,
		"budget:lock:campaign:" + idStr,
		"budget:txid:campaign:" + idStr,
		"campaign:settings:" + idStr,
		PlacementBlacklistKey(id),
	}
}

// SourceOnlyKeys returns keys that must not be copied to the target shard (fence blocks source debits).
func (c *CampaignRedisKeyCatalog) SourceOnlyKeys(id uuid.UUID) []string {
	return []string{MigrationFenceRedisKey(id)}
}

// PrefixPatterns returns SCAN prefixes for variable-cardinality campaign keys.
func (c *CampaignRedisKeyCatalog) PrefixPatterns(id uuid.UUID) []string {
	tag := campaignHashTag(id)
	return []string{
		dailySpendKeyPrefix(id),
		fcapKeyPrefix(id, ""),
		tag + "dup:",
		tag + "dedup/v2:",
		tag + "idempotency:click:",
		tag + "rl:ip:",
		tag + "imp_ts:",
	}
}

// ActivationRequiredKeys returns keys that must exist on the target shard before cutover.
func (c *CampaignRedisKeyCatalog) ActivationRequiredKeys(id uuid.UUID) []string {
	return []string{budgetCampaignKey(id)}
}

// VerifyRequiredKeysExist checks EXISTS on target for activation-required keys.
func (c *CampaignRedisKeyCatalog) VerifyRequiredKeysExist(ctx context.Context, dst redis.Cmdable, id uuid.UUID) error {
	for _, key := range c.ActivationRequiredKeys(id) {
		n, err := dst.Exists(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("exists %q: %w", key, err)
		}
		if n == 0 {
			return fmt.Errorf("required key %q missing on target shard", key)
		}
	}
	return nil
}

// VerifySlotCampaignKeysExist validates activation-required keys for every campaign in a slot.
func (c *CampaignRedisKeyCatalog) VerifySlotCampaignKeysExist(
	ctx context.Context,
	dst redis.Cmdable,
	campaignIDs []uuid.UUID,
) error {
	for _, id := range campaignIDs {
		if err := c.VerifyRequiredKeysExist(ctx, dst, id); err != nil {
			return err
		}
	}
	return nil
}
