package ads

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sqlc querier stub with configurable campaign budgets for registry tests.
type MockRepo struct {
	db.Querier
	ids     []pgtype.UUID
	err     error
	budgets map[uuid.UUID]db.GetCampaignBudgetRow
}

func (m *MockRepo) ListActiveCampaigns(ctx context.Context) ([]db.Campaign, error) {
	var res []db.Campaign
	for _, id := range m.ids {
		res = append(res, db.Campaign{
			ID:         id,
			CustomerID: id,
			Status:     db.CampaignStatusTypeACTIVE,
		})
	}
	return res, m.err
}

func (m *MockRepo) GetCampaignBudget(ctx context.Context, id pgtype.UUID) (db.GetCampaignBudgetRow, error) {
	if m.err != nil {
		return db.GetCampaignBudgetRow{}, m.err
	}
	uid := uuid.UUID(id.Bytes)
	if m.budgets != nil {
		if row, ok := m.budgets[uid]; ok {
			return row, nil
		}
	}
	return db.GetCampaignBudgetRow{
		ID:           id,
		CustomerID:   id,
		BudgetLimit:  1000,
		CurrentSpend: 100,
		Status:       db.CampaignStatusTypeACTIVE,
	}, nil
}

// Builds campaign registry backed by test repository.
func newTestRegistry(t *testing.T, repo db.Querier) *CampaignRegistry {
	t.Helper()
	r := NewRegistry(repo)
	r.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	return r
}

// Guards registry sync loads active campaigns from Postgres into memory.
func TestRegistry_Sync(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	mock := &MockRepo{
		ids: []pgtype.UUID{
			{Bytes: id1, Valid: true},
			{Bytes: id2, Valid: true},
		},
	}

	r := newTestRegistry(t, mock)
	count, err := r.Sync(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.True(t, r.Exists(id1))
	assert.True(t, r.Exists(id2))
	assert.False(t, r.Exists(uuid.New()))
}

// Guards periodic sync keeps registry fresh without blocking readers.
func TestRegistry_StartSync(t *testing.T) {
	id1 := uuid.New()
	mock := &MockRepo{
		ids: []pgtype.UUID{{Bytes: id1, Valid: true}},
	}

	r := newTestRegistry(t, mock)
	ctx, cancel := context.WithCancel(context.Background())

	r.StartSync(ctx, 10*time.Millisecond)

	assert.Eventually(t, func() bool {
		return r.Exists(id1)
	}, 200*time.Millisecond, 20*time.Millisecond)

	cancel()
	r.Wait(ctx)
}
