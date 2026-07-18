package ingestion

import (
	"context"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/metrics"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingCampaignRepo struct {
	slowCampaignRepo
	getByIDCalls int
}

func (r *countingCampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*campaignmodel.Campaign, error) {
	r.getByIDCalls++
	return r.slowCampaignRepo.GetByID(ctx, id)
}

// TestUnifiedFilter_PGFallbackDisabled_NoGetByIDOnCacheMiss guards production hot path from SQL.
func TestUnifiedFilter_PGFallbackDisabled_NoGetByIDOnCacheMiss(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	reg := &mockRegistry{}
	staticCampaign.ID = campID
	staticCampaign.CustomerID = custID
	staticCampaign.IDStr = campID.String()
	staticCampaign.CustomerIDStr = custID.String()
	staticCampaign.IDStrAny = campID.String()
	staticCampaign.CustomerIDStrAny = custID.String()
	staticCampaign.DailyBudgetMicroAny = int64(10_000_000)
	staticCampaign.Location = time.UTC

	repo := &countingCampaignRepo{slowCampaignRepo: slowCampaignRepo{delay: 0}}
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&budgetMissRedis{}},
		NewJumpHashSharder(1),
		reg,
		repo,
		1000,
		time.Minute,
		time.Hour,
		time.Hour,
		1_000_000,
		10_000,
		"events-pg-fallback-off",
		10000,
	)
	f.SetPGFallbackAllowed(false)

	beforePG := testutil.ToFloat64(metrics.BudgetCacheMissPGTotal)
	err := f.Check(context.Background(), &campaignmodel.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		Type:       "click",
		IP:         "1.1.1.1",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBudgetExhausted)
	assert.Equal(t, 0, repo.getByIDCalls)
	assert.Equal(t, beforePG, testutil.ToFloat64(metrics.BudgetCacheMissPGTotal))
}
