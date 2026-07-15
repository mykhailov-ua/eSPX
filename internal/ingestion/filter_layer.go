package ingestion

import (
	"context"

	"espx/internal/campaignmodel"
	redis "github.com/redis/go-redis/v9"
)

const fraudBlacklistKey = "blacklist:fraud"

// FraudLayer is the cascaded anti-fraud outcome after signal accumulation.
type FraudLayer uint8

const (
	FraudLayerNone FraudLayer = iota
	FraudLayerL2Shadow
	FraudLayerL1Reject
)

// decideFraudLayer maps accumulated signals and score tier to L1/L2/L3 handling.
func decideFraudLayer(acc *fraudAccumulator, tier FraudTier) FraudLayer {
	if acc == nil || acc.count == 0 {
		return FraudLayerNone
	}
	if acc.hasFlags(fraudSignalL3) {
		return FraudLayerL1Reject
	}
	if acc.countFlags(fraudSignalL1High) >= 2 {
		return FraudLayerL1Reject
	}
	if acc.countFlags(fraudSignalL1High) >= 1 ||
		acc.countFlags(fraudSignalL2Weak) >= 1 ||
		tier == FraudTierSuspect ||
		tier == FraudTierIVT ||
		tier == FraudTierBlock {
		return FraudLayerL2Shadow
	}
	return FraudLayerNone
}

// applyFraudLayerDecision finalizes score/reason and applies L1/L2/L3 on the event.
func applyFraudLayerDecision(evt *campaignmodel.Event, acc *fraudAccumulator, camp *campaignmodel.Campaign, boost uint8) (FraudLayer, error) {
	if evt == nil {
		return FraudLayerNone, nil
	}
	evt.ShadowEvent = false

	if acc != nil && boost > 0 && !acc.boostApplied {
		sum := acc.score + uint32(boost)
		if sum > 100 {
			sum = 100
		}
		acc.score = sum
		acc.boostApplied = true
	}

	tier := applyFraudAccumulatorForCampaign(evt, acc, camp)
	if acc == nil || acc.count == 0 {
		return FraudLayerNone, nil
	}

	layer := decideFraudLayer(acc, tier)
	recordFraudMetrics(acc, tier, layer)

	switch layer {
	case FraudLayerL1Reject:
		return FraudLayerL1Reject, ErrFraudDetected
	case FraudLayerL2Shadow:
		evt.ShadowEvent = true
		return FraudLayerL2Shadow, nil
	default:
		return FraudLayerNone, nil
	}
}

// FraudBlacklistFilter flags cold-path L3 quarantine hits replicated to blacklist:fraud.
type FraudBlacklistFilter struct {
	rdb redis.UniversalClient
}

// NewFraudBlacklistFilter checks shard-0 blacklist:fraud populated by management outbox replication.
func NewFraudBlacklistFilter(rdb redis.UniversalClient) *FraudBlacklistFilter {
	if rdb == nil {
		return nil
	}
	return &FraudBlacklistFilter{rdb: rdb}
}

// Check records an L3 signal when the client IP is on the replicated fraud blocklist.
func (f *FraudBlacklistFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	if f == nil || evt == nil || evt.IP == "" {
		return nil
	}
	onList, err := f.rdb.SIsMember(ctx, fraudBlacklistKey, evt.IP).Result()
	if err != nil {
		return nil
	}
	if onList {
		addFraudSignal(evt, FraudReasonL3Blocklist)
	}
	return nil
}
