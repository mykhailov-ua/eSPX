package ivtdetector

import (
	"context"
	"testing"

	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMLManagement struct {
	enqueued []struct {
		action     string
		ip         string
		campaignID string
		score      float64
		boost      int32
		ttlSeconds int64
	}
}

func (m *mockMLManagement) BlockIP(ctx context.Context, ip string) error {
	return nil
}

func (m *mockMLManagement) EnqueueMLThreat(ctx context.Context, action string, ip string, campaignID string, score float64, boost int32, ttlSeconds int64) error {
	m.enqueued = append(m.enqueued, struct {
		action     string
		ip         string
		campaignID string
		score      float64
		boost      int32
		ttlSeconds int64
	}{action, ip, campaignID, score, boost, ttlSeconds})
	return nil
}

func TestDetector_MLBoostEnforcement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	// Setup a stub analyzer that returns one boost candidate and one standard candidate.
	campaignID := uuid.New().String()
	stub := stubFinder{
		ips: []SuspiciousIP{
			{
				IP:         "1.2.3.4",
				Reason:     "lgbm-v1",
				Score:      45.0,
				CampaignID: campaignID,
				Action:     "boost",
				Boost:      45,
				TTLSeconds: 300,
			},
			{
				IP:     "5.6.7.8",
				Reason: "high_click_to_imp_ratio",
				Score:  90.0,
			},
		},
	}

	mgmt := &mockMLManagement{}
	idem := NewIdempotencyStore(pool)
	detector := NewDetector(stub, idem, mgmt, pool, DetectorConfig{})

	ctx := context.Background()
	res, err := detector.Run(ctx)
	require.NoError(t, err)

	assert.Equal(t, 2, res.Candidates)
	assert.Equal(t, 2, res.Enqueued)

	// Verify that the boost candidate was enqueued to management via EnqueueMLThreat
	require.Len(t, mgmt.enqueued, 1)
	assert.Equal(t, "boost", mgmt.enqueued[0].action)
	assert.Equal(t, "1.2.3.4", mgmt.enqueued[0].ip)
	assert.Equal(t, campaignID, mgmt.enqueued[0].campaignID)
	assert.Equal(t, 45.0, mgmt.enqueued[0].score)
	assert.Equal(t, int32(45), mgmt.enqueued[0].boost)
	assert.Equal(t, int64(300), mgmt.enqueued[0].ttlSeconds)

	// Verify that the boost candidate cannot be enqueued again (idempotency)
	res2, err := detector.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res2.Candidates)
	assert.Equal(t, 0, res2.Enqueued)
	assert.Equal(t, 2, res2.Skipped)
}
