package ingestion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthz_ZeroAllocs(t *testing.T) {
	h := &AdsPacketHandler{}
	allocs := testing.AllocsPerRun(1000, func() {
		h.healthzHits.Add(1)
		_ = respHealthzOK
	})
	assert.Equal(t, float64(0), allocs)
}

func TestGnetHealthz_IncrementsCounter(t *testing.T) {
	h := &AdsPacketHandler{}
	require.Equal(t, uint64(0), h.HealthzHits())
	h.healthzHits.Add(1)
	require.Equal(t, uint64(1), h.HealthzHits())
}

func TestTrackerReadyz_RedisDown_503(t *testing.T) {
	t.Parallel()

	h := &AdsPacketHandler{}
	h.SetHealthProbeState(false)
	status, body := GetHealthGnet(h)
	require.Equal(t, 503, status)
	assert.Contains(t, body, "not ready")
}
