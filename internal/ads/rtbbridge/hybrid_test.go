package rtbbridge

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"espx/internal/domain"
	"espx/internal/rtb"
)

func TestRtbCampaignInputFromHybrid_ctrPPM(t *testing.T) {
	meta := &CampaignMeta{BidMicro: 500, CTR: 0.05}
	input := RtbCampaignInputFromHybrid(meta, 7, 1, 1, 10, PacingOpenFromManagement(true), 0, 0, 0)
	assert.Equal(t, int64(500), input.BidMicro)
	assert.Equal(t, uint32(50_000), input.CTRPPM)
}

func TestBuildRtbCatalogRowsFromHybrid_overridesBid(t *testing.T) {
	id := uuid.New()
	camp := &domain.Campaign{ID: id, BudgetLimit: 1000}
	meta := &CampaignMeta{BidMicro: 250, CTR: 0.1}
	rows := BuildRtbCatalogRowsFromHybrid(
		[]*domain.Campaign{camp},
		map[uuid.UUID]*CampaignMeta{id: meta},
		map[uuid.UUID]RtbCampaignInput{id: {GeoHash: 7, DeviceMask: 1, CategoryMask: 1, PacingOpen: 1}},
	)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(250), rows[0].Bid)
	assert.Equal(t, uint32(100_000), rows[0].CTRPPM)
}

func TestPacingOpenFromManagement(t *testing.T) {
	assert.Equal(t, uint8(1), PacingOpenFromManagement(true))
	assert.Equal(t, rtb.PacingClosed, PacingOpenFromManagement(false))
}
