package rtbbridge

import (
	"espx/internal/ads/filter"
	"espx/internal/rtb"
)

// NoBidToRejectKind maps RTB no-bid reasons to tracker filter reject kinds for metrics and HTTP.
func NoBidToRejectKind(reason rtb.NoBidReason) filter.FilterRejectKind {
	switch reason {
	case rtb.NoBidPacingClosed:
		return filter.FilterRejectPacing
	case rtb.NoBidDailyCapExceeded, rtb.NoBidSpendFailed:
		return filter.FilterRejectBudget
	case rtb.NoBidNoCandidates, rtb.NoBidEmptyShard:
		return filter.FilterRejectBidFloor
	case rtb.NoBidCorruptCatalog, rtb.NoBidInvalidRequest:
		return filter.FilterRejectInfra
	default:
		return filter.FilterRejectBidFloor
	}
}

// noBidToRejectKind is kept for rtbbridge internals during migration.
func noBidToRejectKind(reason rtb.NoBidReason) filter.FilterRejectKind {
	return NoBidToRejectKind(reason)
}
