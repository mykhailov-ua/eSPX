package management

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestComputeWeightedUnitsFromRows_goldenFixture(t *testing.T) {
	custA := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	campA := uuid.MustParse("10000000-0000-4000-8000-000000000001")
	campB := uuid.MustParse("10000000-0000-4000-8000-000000000002")

	rows := []rollupRow{
		{CampaignID: campA, EventType: "click", Count: 1000},
		{CampaignID: campA, EventType: "duplicate", Count: 100},
		{CampaignID: campB, EventType: "ebpf_drop", Count: 500},
	}
	customers := map[uuid.UUID]uuid.UUID{
		campA: custA,
		campB: custA,
	}

	got := ComputeWeightedUnitsFromRows(rows, customers)
	assert.Equal(t, int64(1010), got[custA]) // 1000*1.0 + 100*0.1 + 500*0.0
}
