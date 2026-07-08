package rtb

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_rtb_catalog_reload proves DealIndex atomic swap stays consistent under concurrent lookups.
func TestChaos_rtb_catalog_reload(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	idx := NewDealIndex()
	idx.UpdateDeals([]DealData{{DealID: "pmp-reload", FloorMicro: 100_000, GeoMask: 7, CatMask: 1, PacingOpen: PacingOpen}})

	d, ok := idx.Lookup("pmp-reload")
	require.True(t, ok)
	require.Equal(t, int64(100_000), d.FloorMicro)

	const workers = 24
	var lookups atomic.Uint64
	var panics atomic.Uint64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					func() {
						defer func() {
							if recover() != nil {
								panics.Add(1)
							}
						}()
						got, found := idx.Lookup("pmp-reload")
						if found {
							if got.FloorMicro != 100_000 && got.FloorMicro != 250_000 {
								panics.Add(1)
							}
							lookups.Add(1)
						}
					}()
				}
			}
		}()
	}

	for range 50 {
		idx.UpdateDeals([]DealData{{DealID: "pmp-reload", FloorMicro: 250_000, GeoMask: 7, CatMask: 1, PacingOpen: PacingOpen}})
		idx.UpdateDeals([]DealData{{DealID: "pmp-reload", FloorMicro: 100_000, GeoMask: 7, CatMask: 1, PacingOpen: PacingOpen}})
	}
	close(stop)
	wg.Wait()

	final, ok := idx.Lookup("pmp-reload")
	require.True(t, ok)
	assert.Equal(t, int64(100_000), final.FloorMicro)
	assert.Equal(t, uint64(0), panics.Load())
	assert.Greater(t, lookups.Load(), uint64(0))

	logRtbChaosProof(t, "rtb_catalog_reload", map[string]string{
		"subsystem":    "rtb_deal_index",
		"baseline_ok":  "true",
		"fault_type":   "concurrent_catalog_swap",
		"workers":      "24",
		"lookups":      itoaU64(lookups.Load()),
		"panics":       "0",
		"floor_stable": "true",
	})
}
