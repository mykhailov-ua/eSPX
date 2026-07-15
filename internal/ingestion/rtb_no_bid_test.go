package ingestion

import (
	"testing"

	"espx/internal/rtb"
	"github.com/stretchr/testify/assert"
)

func TestNoBidToRejectKind(t *testing.T) {
	assert.Equal(t, filterRejectPacing, noBidToRejectKind(rtb.NoBidPacingClosed))
	assert.Equal(t, filterRejectBudget, noBidToRejectKind(rtb.NoBidDailyCapExceeded))
	assert.Equal(t, filterRejectBudget, noBidToRejectKind(rtb.NoBidSpendFailed))
	assert.Equal(t, filterRejectBidFloor, noBidToRejectKind(rtb.NoBidNoCandidates))
	assert.Equal(t, filterRejectInfra, noBidToRejectKind(rtb.NoBidCorruptCatalog))
}
