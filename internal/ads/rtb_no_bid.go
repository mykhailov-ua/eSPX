package ads

import "espx/internal/rtb"

// noBidToRejectKind maps RTB no-bid reasons to tracker filter reject kinds for metrics and HTTP.
func noBidToRejectKind(reason rtb.NoBidReason) filterRejectKind {
	switch reason {
	case rtb.NoBidPacingClosed:
		return filterRejectPacing
	case rtb.NoBidDailyCapExceeded, rtb.NoBidSpendFailed:
		return filterRejectBudget
	case rtb.NoBidNoCandidates, rtb.NoBidEmptyShard:
		return filterRejectBidFloor
	case rtb.NoBidCorruptCatalog, rtb.NoBidInvalidRequest:
		return filterRejectInfra
	default:
		return filterRejectBidFloor
	}
}
