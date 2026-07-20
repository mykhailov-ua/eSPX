package ingestion

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type outageCampaignRepo struct {
	*CampaignRepo
	fail bool
}

func (r *outageCampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	if r.fail {
		return errors.New("simulated pg outage")
	}
	return r.CampaignRepo.UpdateSpend(ctx, id, amount, txID)
}

func (r *outageCampaignRepo) UpdateSpendBatch(ctx context.Context, items []SpendFlushItem) ([]SpendFlushOutcome, error) {
	if r.fail {
		return nil, errors.New("simulated pg outage")
	}
	return r.CampaignRepo.UpdateSpendBatch(ctx, items)
}

func (r *outageCampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status campaignmodel.CampaignStatus) error {
	return r.CampaignRepo.UpdateStatus(ctx, id, status)
}

func (r *outageCampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*campaignmodel.Campaign, error) {
	return r.CampaignRepo.GetByID(ctx, id)
}

func (r *outageCampaignRepo) ListActive(ctx context.Context) ([]*campaignmodel.Campaign, error) {
	return r.CampaignRepo.ListActive(ctx)
}

// TestChaos_LedgerBatch_PGOutage_RollupRetained ensures failed PG flush keeps the in-memory rollup for retry.
func TestChaos_LedgerBatch_PGOutage_RollupRetained(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	campaignID := seedChaosCampaign(t, infra, newChaosRegistry(t, infra.Queries))

	syncKey := "budget:sync:campaign:" + campaignID.String()
	require.NoError(t, infra.Redis.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())
	require.NoError(t, infra.Redis.Set(ctx, syncKey, 250_000, 0).Err())

	baseRepo := NewCampaignRepoWithDB(infra.Pool, infra.Queries)
	repo := &outageCampaignRepo{CampaignRepo: baseRepo, fail: true}
	worker := NewSyncWorker(infra.Redis, repo, nil, time.Hour, 0, nil, 0)

	worker.SyncAll(ctx)

	worker.rollupMu.Lock()
	pending := len(worker.campaignRollup)
	worker.rollupMu.Unlock()
	assert.Equal(t, 1, pending, "rollup must be retained after PG outage")

	inflight, err := infra.Redis.Get(ctx, "budget:inflight:campaign:"+campaignID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(250_000), inflight, "inflight must remain until successful flush")

	logChaosProof(t, "ledger_batch_pg_outage", map[string]string{
		"subsystem":       "ads_processor",
		"rollup_retained": strconv.FormatBool(pending > 0),
		"inflight_micro":  strconv.FormatInt(inflight, 10),
		"baseline_ok":     "true",
	})
}
