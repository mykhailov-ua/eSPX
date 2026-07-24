package rtb

// NoBidReason classifies why RunAuction returned without clearing a winner.
type NoBidReason uint8

const (
	// NoBidNone means the auction cleared successfully.
	NoBidNone NoBidReason = iota
	// NoBidInvalidRequest rejects nil requests or negative publisher floors.
	NoBidInvalidRequest
	// NoBidEmptyShard means the geo shard has no campaigns loaded.
	NoBidEmptyShard
	// NoBidCorruptCatalog means shard Count exceeds backing slice lengths.
	NoBidCorruptCatalog
	// NoBidNoCandidates means no campaign passed targeting, bid, and budget filters.
	NoBidNoCandidates
	// NoBidSpendFailed means the winner lost the final CAS budget debit.
	NoBidSpendFailed
	// NoBidPacingClosed means the campaign pacing gate is closed by management.
	NoBidPacingClosed
	// NoBidDailyCapExceeded means the campaign exceeded its in-memory daily cap snapshot.
	NoBidDailyCapExceeded
	// NoBidTimeout means the auction exceeded the request tmax deadline.
	NoBidTimeout
	// NoBidDealMismatch means the PMP deal rejected geo, category, pacing, or seats.
	NoBidDealMismatch
	// NoBidScanLimit means the candidate scan budget was exhausted before a winner cleared.
	NoBidScanLimit
	// NoBidPrebidIVT means pre-bid IVT gate rejected the request before auction.
	NoBidPrebidIVT
	// NoBidSchainInvalid means supply-chain validation failed on the bid request.
	NoBidSchainInvalid
	// NoBidBreakerOpen means the global emergency breaker blocked the auction.
	NoBidBreakerOpen
)

// OK reports whether the auction cleared.
func (reason NoBidReason) OK() bool {
	return reason == NoBidNone
}

// String returns a stable Prometheus label value for the reason.
func (reason NoBidReason) String() string {
	switch reason {
	case NoBidNone:
		return "ok"
	case NoBidInvalidRequest:
		return "invalid_request"
	case NoBidEmptyShard:
		return "empty_shard"
	case NoBidCorruptCatalog:
		return "corrupt_catalog"
	case NoBidNoCandidates:
		return "no_candidates"
	case NoBidSpendFailed:
		return "spend_failed"
	case NoBidPacingClosed:
		return "pacing_closed"
	case NoBidDailyCapExceeded:
		return "daily_cap"
	case NoBidTimeout:
		return "timeout"
	case NoBidDealMismatch:
		return "deal_mismatch"
	case NoBidScanLimit:
		return "scan_limit"
	case NoBidPrebidIVT:
		return "prebid_ivt"
	case NoBidSchainInvalid:
		return "schain_invalid"
	case NoBidBreakerOpen:
		return "breaker_open"
	default:
		return "unknown"
	}
}
