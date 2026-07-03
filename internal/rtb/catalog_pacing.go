package rtb

const (
	// PacingOpen means the campaign may spend on the bid path.
	PacingOpen uint8 = 1
	// PacingClosed means management closed the pacing gate for this campaign.
	// Must not be zero: unset catalog rows default to open via normalizePacingOpen.
	PacingClosed uint8 = 2
)

// normalizePacingOpen maps unset/zero to open for legacy catalog rows.
func normalizePacingOpen(open uint8) uint8 {
	if open == PacingClosed {
		return PacingClosed
	}
	return PacingOpen
}
