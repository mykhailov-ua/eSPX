package ingestion

import (
	"context"
	"sync"
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/config"

	"github.com/google/uuid"
)

// TestFilterFraudBoost_ConcurrentReload hammers snapshot swap while Check runs (M6-09).
func TestFilterFraudBoost_ConcurrentReload(t *testing.T) {
	cfg := &config.Config{}
	sw := NewSettingsWatcher(nil, cfg)
	campID := uuid.New()

	engine := NewFilterEngine(0, &fraudSignalsFilter{first: FraudReasonMissingImpTS})
	engine.SetRegistry(&mockRegistry{})
	engine.SetSettingsWatcher(sw)

	cachedMockCamp.Store(&campaignmodel.Campaign{ID: campID})
	t.Cleanup(func() { cachedMockCamp.Store(nil) })

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			select {
			case <-stop:
				return
			default:
				sw.fraudScoreBoosts.Store(&FraudScoreBoostSnapshot{
					Boosts: map[uuid.UUID]uint8{campID: uint8(i % 50)},
				})
			}
		}
	}()

	wg.Add(8)
	for w := 0; w < 8; w++ {
		go func() {
			defer wg.Done()
			evt := &campaignmodel.Event{
				CampaignID:   campID,
				StringBuffer: make([]byte, 0, 64),
			}
			ctx := context.Background()
			for i := 0; i < 200; i++ {
				resetFraudBenchEvent(evt)
				_ = engine.Check(ctx, evt)
			}
		}()
	}
	wg.Wait()
	close(stop)
}
