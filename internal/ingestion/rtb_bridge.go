package ingestion

import (
	"encoding/binary"
	"hash/crc32"

	"espx/internal/campaignmodel"
	"espx/internal/rtb"

	"github.com/google/uuid"
)

// BudgetAuthority documents which component owns live spend during RTB rollout.
type BudgetAuthority uint8

const (
	// BudgetAuthorityRedis keeps unified-filter.lua as the authoritative budget debit (production default).
	BudgetAuthorityRedis BudgetAuthority = iota
	// BudgetAuthorityRTB makes rtb.CheckAndSpend authoritative on the bid path; Redis is reconciled async.
	BudgetAuthorityRTB
	// BudgetAuthorityShadow runs in-process auctions for metrics without debiting rtb or Redis.
	BudgetAuthorityShadow
)

// RtbCampaignInput carries per-campaign auction catalog fields not present on campaignmodel.Campaign.
type RtbCampaignInput struct {
	BidMicro         int64
	CTRPPM           uint32
	ReserveMicro     int64
	DailyBudgetMicro int64
	PacingOpen       uint8
	CustomerID       rtb.CustomerID
	CustomerBudget   int64
	DeviceMask       uint8
	CategoryMask     uint64
	GeoHash          uint32
	Weight           uint32
	BoostPPM         uint32
}

// RtbTargetingInput carries request-side auction dimensions derived from ingest metadata.
type RtbTargetingInput struct {
	GeoHash             uint32
	DeviceType          uint8
	CategoryMask        uint64
	PublisherFloorMicro int64
	DealID              string
	DealIDLen           uint8
	DealIDBuf           [64]byte
	SeatCount           uint8
	DeadlineMono        int64
	DealBlock           rtb.NoBidReason
	Schain              SchainNodes
	SchainCount         uint8
}

// CampaignIDFromUUID maps campaign UUIDs to the fixed-width rtb catalog key.
func CampaignIDFromUUID(id uuid.UUID) rtb.CampaignID {
	return rtb.CampaignID(binary.BigEndian.Uint64(id[:8]))
}

// GeoHashFromCountry hashes ISO country codes for rtb geo sharding.
func GeoHashFromCountry(country string) uint32 {
	if country == "" {
		return 0
	}
	return crc32.ChecksumIEEE([]byte(country))
}

// DeviceMaskFromType maps ingest device_type strings to a single device bit.
func DeviceMaskFromType(deviceType []byte) uint8 {
	switch len(deviceType) {
	case 6:
		if deviceType[0] == 'm' && deviceType[1] == 'o' && deviceType[2] == 'b' &&
			deviceType[3] == 'i' && deviceType[4] == 'l' && deviceType[5] == 'e' {
			return 2
		}
		if deviceType[0] == 't' && deviceType[1] == 'a' && deviceType[2] == 'b' &&
			deviceType[3] == 'l' && deviceType[4] == 'e' && deviceType[5] == 't' {
			return 4
		}
	case 7:
		if deviceType[0] == 'd' && deviceType[1] == 'e' && deviceType[2] == 's' &&
			deviceType[3] == 'k' && deviceType[4] == 't' && deviceType[5] == 'o' &&
			deviceType[6] == 'p' {
			return 1
		}
	}
	return 1
}

// BidRequestFromEvent builds an rtb.BidRequest from ingest state without heap allocation.
func BidRequestFromEvent(evt *campaignmodel.Event, targeting RtbTargetingInput) rtb.BidRequest {
	return rtb.BidRequest{
		CategoryMask: targeting.CategoryMask,
		MinBid:       targeting.PublisherFloorMicro,
		GeoHash:      targeting.GeoHash,
		DeviceType:   targeting.DeviceType,
		DeadlineMono: targeting.DeadlineMono,
		DealBlock:    targeting.DealBlock,
	}
}

// CampaignDataFromDomain converts a hot-path campaign view plus auction input into rtb catalog rows.
func CampaignDataFromDomain(camp *campaignmodel.Campaign, input RtbCampaignInput) rtb.CampaignData {
	remaining := camp.BudgetLimit - camp.CurrentSpend
	if remaining < 0 {
		remaining = 0
	}
	return rtb.CampaignData{
		ID:             CampaignIDFromUUID(camp.ID),
		Bid:            input.BidMicro,
		CTRPPM:         input.CTRPPM,
		Reserve:        input.ReserveMicro,
		DailyBudget:    input.DailyBudgetMicro,
		PacingOpen:     input.PacingOpen,
		CustomerID:     input.CustomerID,
		CustomerBudget: input.CustomerBudget,
		DeviceMask:     input.DeviceMask,
		CategoryMask:   input.CategoryMask,
		GeoHashVal:     input.GeoHash,
		Weight:         input.Weight,
		BoostPPM:       input.BoostPPM,
		Budget:         remaining,
	}
}

// BuildRtbCatalogRows materializes rtb.CampaignData rows from active campaigns and per-campaign inputs.
func BuildRtbCatalogRows(campaigns []*campaignmodel.Campaign, inputs map[uuid.UUID]RtbCampaignInput) []rtb.CampaignData {
	if len(campaigns) == 0 {
		return nil
	}
	out := make([]rtb.CampaignData, 0, len(campaigns))
	for _, camp := range campaigns {
		if camp == nil {
			continue
		}
		input, ok := inputs[camp.ID]
		if !ok {
			continue
		}
		out = append(out, fanOutRtbCatalogRows(camp, input)...)
	}
	return out
}
