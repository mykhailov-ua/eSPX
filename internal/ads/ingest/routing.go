package ingest

import (
	"espx/internal/ads/sharding"
)

// ComputeCompositeHashFromTrackReq hashes campaign_id + user_id from a parsed track request.
func ComputeCompositeHashFromTrackReq(req *TrackRequest) uint32 {
	if req == nil {
		return 0
	}
	return sharding.ComputeCompositeHashUUID(req.CampaignID, []byte(req.UserID))
}
