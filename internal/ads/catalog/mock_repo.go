package catalog

import (
	"context"
	"path/filepath"
	"testing"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// MockRepo is a sqlc querier stub with configurable campaign budgets for registry tests.
type MockRepo struct {
	db.Querier
	IDs     []pgtype.UUID
	Err     error
	Budgets map[uuid.UUID]db.GetCampaignBudgetRow
	Full    map[uuid.UUID]db.Campaign
}

func (m *MockRepo) GetCampaignFull(ctx context.Context, id pgtype.UUID) (db.Campaign, error) {
	if m.Err != nil {
		return db.Campaign{}, m.Err
	}
	uid := uuid.UUID(id.Bytes)
	if m.Full != nil {
		if row, ok := m.Full[uid]; ok {
			return row, nil
		}
	}
	if m.Budgets != nil {
		if row, ok := m.Budgets[uid]; ok {
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
	for _, id := range m.IDs {
		res = append(res, db.Campaign{
			ID:         id,
			CustomerID: id,
			Status:     db.CampaignStatusTypeACTIVE,
		})
	}
	return res, m.Err
}

func (m *MockRepo) GetCampaignBudget(ctx context.Context, id pgtype.UUID) (db.GetCampaignBudgetRow, error) {
	if m.Err != nil {
		return db.GetCampaignBudgetRow{}, m.Err
	}
	uid := uuid.UUID(id.Bytes)
	if m.Budgets != nil {
		if row, ok := m.Budgets[uid]; ok {
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

// NewTestRegistry builds a campaign registry backed by a test repository.
func NewTestRegistry(t *testing.T, repo db.Querier) *Registry {
	t.Helper()
	r := NewRegistry(repo)
	r.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	return r
}
