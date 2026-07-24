package ingestion

import (
	"espx/internal/campaignmodel"
	"espx/internal/database"

	"github.com/google/uuid"
)

// resolveDebitShard picks the Redis shard for budget debit / Lua EVALSHA.
// Triplet campaigns use the M2 40/40/20 split. When the chosen shard's breaker is
// open (M14-04), traffic reroutes to campaign_routing reserve (then A/B); if no
// healthy alternative exists, returns ErrShardUnavailable — never silent accept.
func (f *UnifiedFilter) resolveDebitShard(campaignID uuid.UUID, userID string, campInfo *campaignmodel.Campaign) (int, error) {
	shard := f.sharder.GetShard(campaignID)
	if campInfo != nil && campInfo.HasTriplet {
		hash := ComputeCompositeHashUUID(campaignID, []byte(userID))
		pct := hash % 100
		if pct < 40 {
			shard = int(campInfo.PrimaryAShard)
		} else if pct < 80 {
			shard = int(campInfo.PrimaryBShard)
		} else {
			shard = int(campInfo.ReserveShard)
		}
	}

	if !f.shardBreakerOpen(shard) {
		return shard, nil
	}

	if campInfo != nil && campInfo.HasTriplet {
		alts := [...]int{
			int(campInfo.ReserveShard),
			int(campInfo.PrimaryAShard),
			int(campInfo.PrimaryBShard),
		}
		for _, alt := range alts {
			if alt == shard {
				continue
			}
			if !f.shardBreakerOpen(alt) {
				return alt, nil
			}
		}
	}
	return 0, ErrShardUnavailable
}

func (f *UnifiedFilter) shardBreakerOpen(shard int) bool {
	if len(f.breakers) == 0 {
		return false
	}
	n := len(f.breakers)
	if n == 0 {
		return false
	}
	idx := shard % n
	if idx < 0 {
		idx = -idx
	}
	b := f.breakers[idx]
	if b == nil {
		return false
	}
	return b.State() == database.CircuitOpen
}

// SetShardBreakers wires per-shard circuit breakers for M14-04 ingest reroute.
func (f *UnifiedFilter) SetShardBreakers(breakers []*database.RedisBreaker) {
	if f == nil {
		return
	}
	f.breakers = breakers
}
