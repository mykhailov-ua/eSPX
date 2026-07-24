package ingestion

import (
	"testing"

	"espx/internal/campaignmodel"

	"github.com/stretchr/testify/require"
)

func TestTCPControlFrame_roundtrip(t *testing.T) {
	secret := []byte("m2-test-secret")
	limits := &UDPControlLimits{NumShards: 2}
	limits.Limits[0] = 10_000
	limits.Limits[1] = 20_000
	var payload [32]byte
	pl := EncodeTCPLimitsPayload(payload[:], limits)

	hdr := &TCPControlHeader{
		MsgType:        TCPMsgSnapshot,
		RoutingEpoch:   42,
		SlotMapVersion: 3,
		TrackerID:      9,
		NumShards:      limits.NumShards,
	}
	var frame [96]byte
	n, err := EncodeTCPControlFrame(frame[:], secret, hdr, payload[:pl])
	require.NoError(t, err)

	var decoded TCPControlHeader
	out, err := DecodeTCPControlFrame(frame[:n], secret, &decoded)
	require.NoError(t, err)
	require.Equal(t, TCPMsgSnapshot, decoded.MsgType)
	require.Equal(t, int64(42), decoded.RoutingEpoch)
	require.Equal(t, int32(3), decoded.SlotMapVersion)
	require.Len(t, out, pl)
}

func TestTCPControlFrame_invalidHMAC(t *testing.T) {
	secret := []byte("m2-test-secret")
	hdr := &TCPControlHeader{MsgType: TCPMsgSnapshot, RoutingEpoch: 1}
	var frame [64]byte
	_, err := EncodeTCPControlFrame(frame[:], secret, hdr, nil)
	require.NoError(t, err)

	var decoded TCPControlHeader
	_, err = DecodeTCPControlFrame(frame[:], []byte("wrong"), &decoded)
	require.ErrorIs(t, err, ErrTCPControlHMAC)
}

func TestTCPAckPayload_roundtrip(t *testing.T) {
	ack := TCPAckPayload{TrackerID: 7, AppliedEpoch: 99, AppliedSlotVer: 4}
	var buf [16]byte
	require.Equal(t, 16, EncodeTCPAckPayload(buf[:], &ack))
	var out TCPAckPayload
	require.True(t, DecodeTCPAckPayload(buf[:], &out))
	require.Equal(t, ack, out)
}

func TestCampaign_LuaRoutingEpoch(t *testing.T) {
	c := &campaignmodel.Campaign{RoutingEpoch: 0, MigrationGen: 5}
	require.Equal(t, int64(5), c.LuaRoutingEpoch())
	c.RoutingEpoch = 8
	require.Equal(t, int64(8), c.LuaRoutingEpoch())
}
