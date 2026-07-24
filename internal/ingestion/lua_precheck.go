package ingestion

import (
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/licensing"

	"github.com/google/uuid"
)

const (
	luaReturnDailyQuota int64 = 12
	luaReturnPlacement  int64 = 14
	// M14-16 branch tags (high nibble 0x2 family in hex docs: 0x14/0x15 decimal 20/21).
	luaReturnTierDegraded int64 = 20 // 0x14 — degraded ok path
	luaReturnFraudSignal  int64 = 21 // 0x15 — fraud signal with accept

	luaPrecheckIngressTTLSec = 28 * 3600
	luaDegradeThresholdNs    = int64(2_000_000) // 2 ms remaining filter budget
)

// luaBranchLabel maps Lua return codes to Prometheus branch labels (M14-16).
func luaBranchLabel(res int64) string {
	switch res {
	case 0:
		return "ok"
	case 1:
		return "rate"
	case 2:
		return "duplicate"
	case 3:
		return "budget"
	case 4:
		return "pacing"
	case 5:
		return "freq"
	case 6:
		return "ttc_low"
	case 7:
		return "ttc_missing"
	case 10:
		return "ttc_bypass"
	case 11:
		return "migration_fence"
	case luaReturnDailyQuota:
		return "daily_quota"
	case luaReturnPlacement:
		return "placement"
	case luaReturnTierDegraded:
		return "tier_degraded"
	case luaReturnFraudSignal:
		return "fraud_signal"
	default:
		return "accept"
	}
}

var (
	luaPrecheckIngressTTLAny any = luaPrecheckIngressTTLSec
	luaDegradeThresholdAny   any = luaDegradeThresholdNs
)

var (
	fraudBlacklistKeyVal   = StringVal{s: fraudBlacklistKey}
	placementIgnoredKeyVal = StringVal{s: "fcap:ignored"}
	ingressIgnoredKeyVal   = StringVal{s: "fcap:ignored"}
)

// maxRPDAnyCache pre-boxes entitlement daily limits passed to Lua pre-checks.
var maxRPDAnyCache [8192]any

func maxRPDAsAny(v uint64) any {
	if v == 0 {
		return zeroAny
	}
	if int(v) < len(maxRPDAnyCache) {
		return maxRPDAnyCache[v]
	}
	return v
}

type entitlementsLookup interface {
	GetEntitlements(customerID uuid.UUID) (licensing.Entitlements, bool)
}

// luaPrecheckScratch holds pooled keys for consolidated Lua pre-checks (M9-02).
type luaPrecheckScratch struct {
	wIngress, wPlacement bufWrapper
	maxRPDAny            any
	ingressTTLAny        any
}

func (f *UnifiedFilter) entitlementsMaxRPD(custID uuid.UUID) uint64 {
	lookup, ok := f.registry.(entitlementsLookup)
	if !ok {
		return 0
	}
	ent, ok := lookup.GetEntitlements(custID)
	if !ok || ent.Limits.MaxRequestsPerDay == 0 {
		return 0
	}
	return ent.Limits.MaxRequestsPerDay
}

func appendCampaignIngressDayKey(dst []byte, campaignID uuid.UUID, regionCode uint8, customerID uuid.UUID, t time.Time) []byte {
	dst = appendCampaignHashTag(dst[:0], campaignID)
	dst = append(dst, "ingress:day:"...)
	if regionCode > 0 {
		dst = append(dst, hexByte(regionCode>>4), hexByte(regionCode&0x0f), ':')
	}
	dst = appendUUID(dst, customerID)
	dst = append(dst, ':')
	return appendDate(dst, t)
}

func (f *UnifiedFilter) fillLuaPrecheckKeys(
	evt *campaignmodel.Event,
	campInfo *campaignmodel.Campaign,
	now time.Time,
	scratch *luaPrecheckScratch,
	kv []StringVal,
	keyArgs []any,
	ingressIdx, placementIdx int,
) (maxRPDAny any) {
	maxRPD := f.entitlementsMaxRPD(campInfo.CustomerID)
	maxRPDAny = zeroAny
	if maxRPD > 0 {
		maxRPDAny = maxRPDAsAny(maxRPD)
		w := &scratch.wIngress
		w.buf = w.buf[:0]
		w.buf = appendCampaignIngressDayKey(w.buf, evt.CampaignID, f.regionCode, campInfo.CustomerID, now)
		kv[ingressIdx].s = unsafeString(w.buf)
		keyArgs[ingressIdx] = &kv[ingressIdx]
	} else {
		keyArgs[ingressIdx] = &ingressIgnoredKeyVal
	}

	if evt.PlacementID != "" {
		w := &scratch.wPlacement
		w.buf = appendCampaignHashTag(w.buf[:0], evt.CampaignID)
		w.buf = append(w.buf, "blacklist:placement:"...)
		w.buf = appendUUID(w.buf, evt.CampaignID)
		kv[placementIdx].s = unsafeString(w.buf)
		keyArgs[placementIdx] = &kv[placementIdx]
	} else {
		keyArgs[placementIdx] = &placementIgnoredKeyVal
	}
	return maxRPDAny
}
