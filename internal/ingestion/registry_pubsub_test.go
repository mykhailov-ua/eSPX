package ingestion

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	db "espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingMockRepo struct {
	MockRepo
	listActiveCalls atomic.Int32
}

func (m *countingMockRepo) ListActiveCampaigns(ctx context.Context) ([]db.ListActiveCampaignsRow, error) {
	m.listActiveCalls.Add(1)
	return m.MockRepo.ListActiveCampaigns(ctx)
}

func TestRegistry_StartWatch_IncrementalOnlyOneCampaign(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	custID := uuid.New()

	mock := &countingMockRepo{
		MockRepo: MockRepo{
			budgets: map[uuid.UUID]db.GetCampaignBudgetRow{
				campID: {
					ID:           pgtype.UUID{Bytes: campID, Valid: true},
					CustomerID:   pgtype.UUID{Bytes: custID, Valid: true},
					BudgetLimit:  8_000_000,
					CurrentSpend: 2_000_000,
					Status:       db.CampaignStatusTypeACTIVE,
				},
			},
		},
	}

	r := newTestRegistry(t, mock)
	r.Add(campID, custID, nil, "", campaignmodel.PacingModeAsap, 1000, "UTC", 0, 0, nil)

	channel := "test:campaign:updates:hr-pub"
	r.StartWatch(ctx, rdb, channel)
	time.Sleep(200 * time.Millisecond)

	before := mock.listActiveCalls.Load()
	require.NoError(t, rdb.Publish(ctx, channel, campID.String()).Err())

	assert.Eventually(t, func() bool {
		camp, ok := r.GetCampaign(campID)
		return ok && camp.BudgetLimit == 8_000_000
	}, 2*time.Second, 50*time.Millisecond)

	assert.Equal(t, before, mock.listActiveCalls.Load(), "pub/sub must not trigger full ListActiveCampaigns sync")
}
