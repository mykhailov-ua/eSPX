package ads

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIngressQuotaMap_tryAcquire_perWorkerLimit(t *testing.T) {
	var limits UDPControlLimits
	limits.NumShards = 2
	limits.Limits[0] = 100
	limits.Limits[1] = 200
	m := buildIngressQuotaMap(1, &limits, 4)
	require.NotNil(t, m)

	// per-worker limit = 100/4 = 25 on shard 0
	for i := 0; i < 25; i++ {
		require.True(t, m.tryAcquire(0, 0))
	}
	require.False(t, m.tryAcquire(0, 0))
	require.True(t, m.tryAcquire(0, 1))
}

func TestIngressQuotaMap_epochSwapResetsCounters(t *testing.T) {
	var limits UDPControlLimits
	limits.NumShards = 1
	limits.Limits[0] = 40
	m1 := buildIngressQuotaMap(1, &limits, 2)
	for i := 0; i < 20; i++ {
		require.True(t, m1.tryAcquire(0, 0))
	}
	m2 := buildIngressQuotaMap(2, &limits, 2)
	require.True(t, m2.tryAcquire(0, 0))
}

func TestUDPControl_TryIngress(t *testing.T) {
	c := NewUDPControl(UDPControlConfig{
		Enabled:    true,
		NumShards:  1,
		NumWorkers: 2,
		InitialRPS: 10,
	})
	for i := 0; i < 5; i++ {
		require.True(t, c.TryIngress(0, 0))
	}
	require.False(t, c.TryIngress(0, 0))
	require.True(t, c.TryIngress(0, 1))
}

func TestIngressQuotaMap_race(t *testing.T) {
	var limits UDPControlLimits
	limits.NumShards = 4
	for i := uint8(0); i < 4; i++ {
		limits.Limits[i] = 10_000
	}
	m := buildIngressQuotaMap(1, &limits, 8)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				_ = m.tryAcquire(i%4, worker)
			}
		}(w)
	}
	wg.Wait()
}
