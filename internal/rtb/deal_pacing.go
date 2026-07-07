package rtb

import (
	"errors"
	"strings"
)

var ErrInvalidDealPacing = errors.New("pacing must be open or closed")

// ParseDealPacingString maps admin/API pacing labels to rtb_deals.pacing (int16).
func ParseDealPacingString(v string) (int16, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "open":
		return int16(PacingOpen), nil
	case "closed":
		return int16(PacingClosed), nil
	default:
		return 0, ErrInvalidDealPacing
	}
}

// DealPacingLabel returns the admin API label for a stored pacing value.
func DealPacingLabel(p int16) string {
	if p == int16(PacingClosed) {
		return "closed"
	}
	return "open"
}

// DealPacingOpen maps rtb_deals.pacing to the uint8 bid-path pacing flag.
func DealPacingOpen(p int16) uint8 {
	if p == int16(PacingClosed) {
		return PacingClosed
	}
	return PacingOpen
}
