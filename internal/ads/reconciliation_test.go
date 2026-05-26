package ads

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
)

type MockCampaignRepository struct {
	campaigns []*domain.Campaign
}

func (m *MockCampaignRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	for _, c := range m.campaigns {
		if c.ID == id {
			return c, nil
		}
	}
	return nil, errors.New("not found")
}

func (m *MockCampaignRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	return nil
}

func (m *MockCampaignRepository) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	return nil
}

func (m *MockCampaignRepository) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	return m.campaigns, nil
}

func TestReconciliationWorker_DataDriftDetection(t *testing.T) {
	ctx := context.Background()

	campID1 := uuid.New()
	campID2 := uuid.New()

	// 1. Setup Mock Campaign Registry
	repo := &MockCampaignRepository{
		campaigns: []*domain.Campaign{
			{ID: campID1, Status: domain.CampaignStatusActive},
			{ID: campID2, Status: domain.CampaignStatusActive},
		},
	}

	// 2. Setup Mock Postgres spends
	// Campaign 1: Postgres spend = 100,000 micro-units
	// Campaign 2: Postgres spend = 200,000 micro-units
	pg := &MockPostgresDB{
		spends: map[uuid.UUID]int64{
			campID1: 100_000,
			campID2: 200_000,
		},
	}
	pg.Healthy.Store(true)

	// 3. Setup Mock ClickHouse logs
	ch := &MockClickHouseDB{}

	// Let's populate ClickHouse logs (T_c - 10 minutes ago, well outside the 5m lag allowance)
	logTime := time.Now().Add(-10 * time.Minute)

	// Campaign 1: Intentionally "lose" one batch of 1,000 micro-units!
	// Real Postgres spend is 100,000, but in ClickHouse we only record 99,000 micro-units (9 clicks, 9 impressions).
	// Expected drift = |100,000 - 99,000| / 100,000 = 1.0% (0.01)
	for i := 0; i < 9; i++ {
		ch.LogEvent(&domain.Event{
			CampaignID: campID1,
			ClickID:    uuid.NewString(),
			Type:       "click",
			CreatedAt:  logTime,
		})
		ch.LogEvent(&domain.Event{
			CampaignID: campID1,
			ClickID:    uuid.NewString(),
			Type:       "impression",
			CreatedAt:  logTime,
		})
	}

	// Campaign 2: Perfectly consistent spends!
	// Real Postgres spend is 200,000, and ClickHouse has exactly 200,000 (18 clicks, 20 impressions).
	// Expected drift = 0.0% (0.0)
	for i := 0; i < 18; i++ {
		ch.LogEvent(&domain.Event{
			CampaignID: campID2,
			ClickID:    uuid.NewString(),
			Type:       "click",
			CreatedAt:  logTime,
		})
	}
	for i := 0; i < 20; i++ {
		ch.LogEvent(&domain.Event{
			CampaignID: campID2,
			ClickID:    uuid.NewString(),
			Type:       "impression",
			CreatedAt:  logTime,
		})
	}

	// 4. Initialize ReconciliationWorker
	// Audit interval = 10 minutes, lag allowance = 5 minutes, drift limit = 0.5% (0.005)
	rw := NewReconciliationWorker(pg, ch, repo, 0.005, 5*time.Minute, 10*time.Minute)

	// 5. Run audit reconciliation pass
	err := rw.Reconcile(ctx)
	require.NoError(t, err)

	// 6. Assert and read Prometheus data drift metrics
	metric1 := &io_prometheus_client.Metric{}
	err = metrics.DataDriftRatio.WithLabelValues(campID1.String()).Write(metric1)
	require.NoError(t, err)
	driftVal1 := metric1.GetGauge().GetValue()

	metric2 := &io_prometheus_client.Metric{}
	err = metrics.DataDriftRatio.WithLabelValues(campID2.String()).Write(metric2)
	require.NoError(t, err)
	driftVal2 := metric2.GetGauge().GetValue()

	// Assertions:
	// Campaign 1 should have a 1% drift ratio (0.01) which is critical (> 0.5%)
	assert.InDelta(t, 0.01, driftVal1, 0.0001, "Campaign 1 should detect 1.0% drift due to lost batch")

	// Campaign 2 should have 0% drift (0.0)
	assert.Equal(t, 0.0, driftVal2, "Campaign 2 should have exactly 0.0% drift")
}
