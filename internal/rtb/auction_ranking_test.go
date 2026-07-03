package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Guards zero CTRPPM normalizes to 1.0 for legacy catalog rows.
func TestEffectiveScore_defaultsCTR(t *testing.T) {
	assert.Equal(t, int64(100), effectiveScore(100, 0))
}
