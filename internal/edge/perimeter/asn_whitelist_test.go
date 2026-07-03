package perimeter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Guards CDN and mobile ASN lists bypass edge blacklist.
func TestASNWhitelist_membership(t *testing.T) {
	w := NewASNWhitelist("15169,20940", "21928")
	assert.True(t, w.IsWhitelisted("15169"))
	assert.True(t, w.IsWhitelisted(" 21928 "))
	assert.False(t, w.IsWhitelisted("64512"))
	assert.False(t, w.IsWhitelisted(""))
}
