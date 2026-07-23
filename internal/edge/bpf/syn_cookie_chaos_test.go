package bpf

import (
	"net"
	"testing"

	"espx/internal/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	xdpDrop = 1
	xdpPass = 2
	xdpTX   = 3
)

// TestChaos_XDPSynCookieReturnCode verifies xdp_syn_cookie does not PASS the
// original SYN to the kernel stack. A successful cookie issues XDP_TX; helper
// absence or malformed input must DROP.
func TestChaos_XDPSynCookieReturnCode(t *testing.T) {
	objs := loadTestObjects(t)
	if objs.XdpSynCookie == nil {
		t.Skip("xdp_syn_cookie not available in this kernel")
	}

	pkt := buildSYNPacket(t, net.IPv4(192, 0, 2, 1), net.IPv4(10, 0, 0, 1), trackerPort)

	ret, _, err := objs.XdpSynCookie.Test(pkt)
	require.NoError(t, err)

	assert.NotEqual(t, xdpPass, ret, "SYN must not reach kernel stack via XDP_PASS")

	cookies := statCount(t, objs.Stats, StatSynCookie)
	if cookies > 0 {
		assert.Equal(t, xdpTX, ret, "cookie issued must return XDP_TX")
		testutil.LogChaosProof(t, "xdp_syn_cookie_mitigation", map[string]string{
			"status": "pass",
			"return": xdpActionLabel(ret),
		})
		return
	}

	assert.Equal(t, xdpDrop, ret, "helper unavailable must DROP, not PASS")
	testutil.LogChaosProof(t, "xdp_syn_cookie_mitigation", map[string]string{
		"status": "helper_unavailable",
		"return": xdpActionLabel(ret),
	})
}

func xdpActionLabel(act uint32) string {
	switch act {
	case 0:
		return "XDP_ABORTED"
	case xdpDrop:
		return "XDP_DROP"
	case xdpPass:
		return "XDP_PASS"
	case xdpTX:
		return "XDP_TX"
	case 4:
		return "XDP_REDIRECT"
	default:
		return "UNKNOWN"
	}
}
