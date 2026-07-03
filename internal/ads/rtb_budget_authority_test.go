package ads

import (
	"context"
	"testing"
	"time"

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
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	f.SetSkipBudgetDebit(true)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)
	before, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)

	evt := &domain.Event{
		Type:       "click",
		CampaignID: campID,
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}
	require.NoError(t, f.Check(attachFilterDeadline(ctx, time.Second), evt))

	after, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, before, after, "skip_budget must not debit Redis campaign budget")
}
