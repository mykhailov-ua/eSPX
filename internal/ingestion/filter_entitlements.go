package ingestion

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/campaignmodel"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

type EntitlementsFilter struct {
	registry   *Registry
	sharder    Sharder
	rdbs       []redis.UniversalClient
	regionCode uint8
}

func NewEntitlementsFilter(registry *Registry, sharder Sharder, rdbs []redis.UniversalClient) *EntitlementsFilter {
	return &EntitlementsFilter{
		registry: registry,
		sharder:  sharder,
		rdbs:     rdbs,
	}
}

// SetRegionCode scopes daily ingress counters to a regional cell (M7).
func (f *EntitlementsFilter) SetRegionCode(code uint8) {
	if f != nil {
		f.regionCode = code
	}
}

func (f *EntitlementsFilter) getRDB(id uuid.UUID) redis.UniversalClient {
	shard := f.sharder.GetShard(id)
	return f.rdbs[shard]
}

func (f *EntitlementsFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	// 1. Get customer ID
	campInfo, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}
	custID := campInfo.CustomerID

	// 2. Check customer subscription entitlements
	ent, ok := f.registry.GetEntitlements(custID)
	if !ok {
		// If subscription doesn't exist, we fall back to open/unlimited
		return nil
	}

	// Feature flag check
	if evt.Type == "bid" || evt.Type == "rtb" {
		if !ent.Features.OpenRTBEnabled() {
			return ErrLicenseExpired
		}
	}

	// 3. RPD daily quota check
	if ent.Limits.MaxRequestsPerDay == 0 {
		return nil
	}

	timezone := ent.Limits.QuotaResetTimezone
	if timezone == "" {
		timezone = "UTC"
	}

	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}

	dateStr := time.Now().In(loc).Format("20060102")

	var keyBuf [128]byte
	b := IngressDayKey(keyBuf[:0], f.regionCode, custID, dateStr)
	redisKey := unsafeString(b)

	rdb := f.getRDB(custID)
	if rdb == nil {
		return nil
	}

	pipe := rdb.Pipeline()
	incr := pipe.Incr(ctx, redisKey)
	pipe.Expire(ctx, redisKey, 28*time.Hour)
	_, err = pipe.Exec(ctx)
	if err != nil {
		slog.Warn("failed to increment daily quota counter in Redis", "customer_id", custID, "error", err)
		return nil
	}

	currentVal := incr.Val()
	if uint64(currentVal) > ent.Limits.MaxRequestsPerDay {
		return ErrDailyQuotaExceeded
	}

	return nil
}
