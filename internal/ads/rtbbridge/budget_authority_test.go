package rtbbridge

import (
	"context"
	"testing"
	"time"

	"espx/internal/ads/filter"
	"espx/internal/ads/testutil"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards Lua skips budget debit when RTB owns authoritative spend.
func TestUnifiedFilter_skipBudgetDebit_preservesRedisBalance(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := testutil.SetupTestRedis(t)
	defer cleanup()

	f := filter.NewTestUnifiedFilter(t, rdb)
	f.SetSkipBudgetDebit(true)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	filter.SeedTestCampaignBudget(t, ctx, rdb, campID)
	before, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)

	evt := &domain.Event{
		Type:       "click",
		CampaignID: campID,
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}
	evt.FilterDeadlineMono = filter.MonotonicNano() + time.Second.Nanoseconds()
	require.NoError(t, f.Check(ctx, evt))

	after, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, before, after, "skip_budget must not debit Redis campaign budget")
}
