package ingestion

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIngressDayKey_regionScoped(t *testing.T) {
	t.Parallel()
	cust := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	key := IngressDayKey(nil, 0x0a, cust, "20260720")
	require.Equal(t, "ingress:day:0a:11111111-1111-1111-1111-111111111111:20260720", string(key))

	keySingle := IngressDayKey(nil, 0, cust, "20260720")
	require.Equal(t, "ingress:day:11111111-1111-1111-1111-111111111111:20260720", string(keySingle))
}

func TestUDPControlLimits_RPDCodec(t *testing.T) {
	t.Parallel()
	var limits UDPControlLimits
	limits.NumShards = 2
	limits.Limits[0] = 10_000
	limits.Limits[1] = 10_000
	limits.MaxRPD = 5_000_000

	var buf [64]byte
	n := udpEncodeShardLimits(buf[:], &limits)
	require.Equal(t, 24, n)

	var decoded UDPControlLimits
	require.True(t, udpDecodeShardLimits(buf[:n], 2, &decoded))
	require.Equal(t, limits.Limits[0], decoded.Limits[0])
	require.Equal(t, limits.MaxRPD, decoded.MaxRPD)
}
