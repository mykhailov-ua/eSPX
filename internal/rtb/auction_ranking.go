package rtb

// CTRPPMUnit is one full CTR (1.0) in parts-per-million fixed point.
const CTRPPMUnit uint32 = 1_000_000

// normalizeCTRPPM maps zero to 1.0 CTR for legacy catalog rows.
func normalizeCTRPPM(ctrPPM uint32) uint32 {
	if ctrPPM == 0 {
		return CTRPPMUnit
	}
	return ctrPPM
}

// effectiveScore returns bid*CTR in micro-score units for ranking only.
func effectiveScore(bid int64, ctrPPM uint32) int64 {
	return effectiveScoreWithBoost(bid, ctrPPM, CTRPPMUnit)
}

// effectiveScoreWithBoost returns bid*CTR*boost in micro-score units (boost PPM defaults to 1.0).
func effectiveScoreWithBoost(bid int64, ctrPPM, boostPPM uint32) int64 {
	ctr := normalizeCTRPPM(ctrPPM)
	boost := normalizeCTRPPM(boostPPM)
	denom := int64(CTRPPMUnit) * int64(CTRPPMUnit)
	return bid * int64(ctr) * int64(boost) / denom
}
