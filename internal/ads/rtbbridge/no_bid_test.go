package rtbbridge

import (
	"testing"

	"espx/internal/ads/filter"
	"espx/internal/rtb"

	"github.com/stretchr/testify/assert"
)

func TestNoBidToRejectKind(t *testing.T) {
	assert.Equal(t, filter.FilterRejectPacing, noBidToRejectKind(rtb.NoBidPacingClosed))
	assert.Equal(t, filter.FilterRejectBudget, noBidToRejectKind(rtb.NoBidDailyCapExceeded))
	assert.Equal(t, filter.FilterRejectBudget, noBidToRejectKind(rtb.NoBidSpendFailed))
	assert.Equal(t, filter.FilterRejectBidFloor, noBidToRejectKind(rtb.NoBidNoCandidates))
	assert.Equal(t, filter.FilterRejectInfra, noBidToRejectKind(rtb.NoBidCorruptCatalog))
}
