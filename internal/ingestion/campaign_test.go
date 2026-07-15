package ingestion

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/ingestion/sqlc"
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
	full    map[uuid.UUID]db.Campaign
}

func (m *MockRepo) GetCampaignFull(ctx context.Context, id pgtype.UUID) (db.Campaign, error) {
	if m.err != nil {
		return db.Campaign{}, m.err
	}
	uid := uuid.UUID(id.Bytes)
	if m.full != nil {
		if row, ok := m.full[uid]; ok {
			return row, nil
		}
	}
	if m.budgets != nil {
		if row, ok := m.budgets[uid]; ok {
			return db.Campaign{
				ID:           row.ID,
				CustomerID:   row.CustomerID,
				BudgetLimit:  row.BudgetLimit,
				CurrentSpend: row.CurrentSpend,
				Status:       row.Status,
			}, nil
		}
	}
	return db.Campaign{
		ID:           id,
		CustomerID:   id,
		BudgetLimit:  1000,
		CurrentSpend: 100,
		Status:       db.CampaignStatusTypeACTIVE,
	}, nil
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

func newTestRegistry(t *testing.T, repo db.Querier) *Registry {
	t.Helper()
	r := NewRegistry(repo)
	r.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	return r
}

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

func TestCampaignFromDBRow_FraudConfig(t *testing.T) {
	id := uuid.New()
	row := db.Campaign{
		ID:                    pgtype.UUID{Bytes: id, Valid: true},
		CustomerID:            pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Status:                db.CampaignStatusTypeACTIVE,
		FraudThresholdPass:    25,
		FraudThresholdSuspect: 55,
		FraudThresholdIvt:     75,
		FraudThresholdBlock:   95,
		GhostIvtEnabled:       true,
		BehaviorFlags:         int32(campaignmodel.BehaviorLowTTC | campaignmodel.BehaviorVelIP),
	}
	camp := campaignFromDBRow(row)
	assert.Equal(t, uint8(25), camp.FraudThresholdPass)
	assert.Equal(t, uint8(55), camp.FraudThresholdSuspect)
	assert.Equal(t, uint8(75), camp.FraudThresholdIVT)
	assert.Equal(t, uint8(95), camp.FraudThresholdBlock)
	assert.True(t, camp.GhostIVTEnabled)
	assert.Equal(t, campaignmodel.BehaviorLowTTC|campaignmodel.BehaviorVelIP, camp.BehaviorFlags)
}
