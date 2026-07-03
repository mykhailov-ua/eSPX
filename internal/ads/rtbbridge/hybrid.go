package rtbbridge

import (
	"encoding/binary"
	"math"

	"espx/internal/domain"
	"espx/internal/rtb"

	"github.com/google/uuid"
)

// CustomerIDFromCustomerUUID maps customer UUIDs to rtb.CustomerID.
func CustomerIDFromCustomerUUID(id uuid.UUID) rtb.CustomerID {
	return rtb.CustomerID(binary.BigEndian.Uint64(id[:8]))
}

// PacingOpenFromManagement maps management pacing state to an rtb catalog gate.
// externallyOpen is false when the pacing controller closed spend for this campaign.
func PacingOpenFromManagement(externallyOpen bool) uint8 {
	if externallyOpen {
		return rtb.PacingOpen
	}
	return rtb.PacingClosed
}

// RtbCampaignInputFromHybrid builds rtb catalog input from HybridBalancer metadata.
// CTR is converted to PPM fixed-point; pacingOpen comes from management, not the balancer.
func RtbCampaignInputFromHybrid(
	meta *CampaignMeta,
	geo uint32,
	deviceMask uint8,
	categoryMask uint64,
	weight uint32,
	pacingOpen uint8,
	customerID rtb.CustomerID,
	customerBudget int64,
	dailyBudget int64,
) RtbCampaignInput {
	ctrPPM := uint32(CTRPPMUnit)
	if meta != nil && meta.CTR > 0 {
		scaled := meta.CTR * float64(CTRPPMUnit)
		if scaled > float64(math.MaxUint32) {
			ctrPPM = math.MaxUint32
		} else {
			ctrPPM = uint32(scaled)
		}
	}
	bidMicro := int64(0)
	if meta != nil {
		bidMicro = meta.BidMicro
	}
	return RtbCampaignInput{
		BidMicro:         bidMicro,
		CTRPPM:           ctrPPM,
		DeviceMask:       deviceMask,
		CategoryMask:     categoryMask,
		GeoHash:          geo,
		Weight:           weight,
		PacingOpen:       pacingOpen,
		CustomerID:       customerID,
		CustomerBudget:   customerBudget,
		DailyBudgetMicro: dailyBudget,
	}
}

// CTRPPMUnit duplicates rtb.CTRPPMUnit for ads-side helpers without exporting rtb constants.
const CTRPPMUnit = 1_000_000

// BuildRtbCatalogRowsFromHybrid builds catalog rows using hybrid metadata for bid/ctr.
func BuildRtbCatalogRowsFromHybrid(
	campaigns []*domain.Campaign,
	metaByID map[uuid.UUID]*CampaignMeta,
	inputs map[uuid.UUID]RtbCampaignInput,
) []rtb.CampaignData {
	if len(campaigns) == 0 {
		return nil
	}
	out := make([]rtb.CampaignData, 0, len(campaigns))
	for _, camp := range campaigns {
		if camp == nil {
			continue
		}
		base, ok := inputs[camp.ID]
		if !ok {
			continue
		}
		meta := metaByID[camp.ID]
		if meta != nil {
			base.BidMicro = meta.BidMicro
			if meta.CTR > 0 {
				scaled := meta.CTR * CTRPPMUnit
				if scaled > math.MaxUint32 {
					base.CTRPPM = math.MaxUint32
				} else {
					base.CTRPPM = uint32(scaled)
				}
			}
		}
		row := CampaignDataFromDomain(camp, base)
		out = append(out, row)
	}
	return out
}
