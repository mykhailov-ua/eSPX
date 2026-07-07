package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDealPacingString(t *testing.T) {
	p, err := ParseDealPacingString("open")
	require.NoError(t, err)
	assert.Equal(t, int16(PacingOpen), p)

	p, err = ParseDealPacingString("closed")
	require.NoError(t, err)
	assert.Equal(t, int16(PacingClosed), p)

	_, err = ParseDealPacingString("burst")
	assert.ErrorIs(t, err, ErrInvalidDealPacing)
}

func TestDealPacingLabelAndOpen(t *testing.T) {
	assert.Equal(t, "open", DealPacingLabel(int16(PacingOpen)))
	assert.Equal(t, "closed", DealPacingLabel(int16(PacingClosed)))
	assert.Equal(t, PacingOpen, DealPacingOpen(int16(PacingOpen)))
	assert.Equal(t, PacingClosed, DealPacingOpen(int16(PacingClosed)))
}
