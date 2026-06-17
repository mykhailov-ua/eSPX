package ads

import (
	"context"
	"strconv"
	"testing"
	"time"

	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards click arriving too fast after impression is flagged as fraud.
func TestUnifiedFilter_LowTTC_ReturnsFraudDetected(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	f.SetTTCMin(300 * time.Millisecond)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	impKey := "imp_ts:user1:" + campID.String()
	require.NoError(t, rdb.Set(ctx, impKey, strconv.FormatInt(time.Now().Add(-50*time.Millisecond).UnixMilli(), 10), 10*time.Minute).Err())

	evt := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: campID,
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}

	err := f.Check(attachFilterDeadline(ctx, time.Second), evt)
	require.ErrorIs(t, err, ErrFraudDetected)
	assert.Contains(t, evt.FraudReason, "low_ttc")
}

// Guards impression sets imp timestamp that click TTC check consumes.
func TestUnifiedFilter_impressionSetsImpTS_clickChecksTTC(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	f.SetTTCMin(50 * time.Millisecond)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	imp := &domain.Event{
		Type:       "impression",
		IP:         "1.1.1.1",
		UserID:     "user123",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	require.NoError(t, f.Check(attachFilterDeadline(ctx, time.Second), imp))

	time.Sleep(60 * time.Millisecond)

	click := &domain.Event{
		Type:       "click",
		IP:         "1.1.1.1",
		UserID:     "user123",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	require.NoError(t, f.Check(attachFilterDeadline(ctx, time.Second), click))
}

// Guards click without prior impression fails closed on real Redis.
func TestUnifiedFilter_failClosed_missingImpTS_realRedis(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	f.SetTTCMin(300 * time.Millisecond)
	f.SetTTCFailClosed(true)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	click := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: campID,
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}
	err := f.Check(attachFilterDeadline(ctx, time.Second), click)
	require.ErrorIs(t, err, ErrFraudDetected)
	assert.Equal(t, "missing_imp_ts", click.FraudReason)
}

// Guards low TTC rejection path with mock Redis Lua responses.
func TestUnifiedFilter_lowTTC_mockRedis(t *testing.T) {
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&lowTTCRedis{}},
		NewJumpHashSharder(1),
		&mockRegistry{},
		nil,
		0,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10_000,
	)
	f.SetTTCMin(300 * time.Millisecond)

	evt := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: uuid.New(),
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}
	err := f.Check(context.Background(), evt)
	require.ErrorIs(t, err, ErrFraudDetected)
	assert.Contains(t, evt.FraudReason, "low_ttc")
}

// Guards missing impression timestamp fails closed with mock Redis.
func TestUnifiedFilter_failClosed_missingImpTS_mockRedis(t *testing.T) {
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&missingImpTSRedis{}},
		NewJumpHashSharder(1),
		&mockRegistry{},
		nil,
		0,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10_000,
	)
	f.SetTTCMin(300 * time.Millisecond)
	f.SetTTCFailClosed(true)

	evt := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: uuid.New(),
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}
	err := f.Check(context.Background(), evt)
	require.ErrorIs(t, err, ErrFraudDetected)
	assert.Equal(t, "missing_imp_ts", evt.FraudReason)
}

// Guards TTC bypass path increments telemetry when impression check skipped.
func TestUnifiedFilter_ttcBypass_incrementsMetric(t *testing.T) {
	before := testutil.ToFloat64(metrics.TTCBypassTotal)
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&ttcBypassRedis{}},
		NewJumpHashSharder(1),
		&mockRegistry{},
		nil,
		0,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10_000,
	)
	f.SetTTCMin(300 * time.Millisecond)

	evt := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: uuid.New(),
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}
	require.NoError(t, f.Check(context.Background(), evt))
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.TTCBypassTotal))
}

// Redis stub returning low TTC Lua result for fraud rejection tests.
type lowTTCRedis struct {
	mockRedisClient
}

func (lowTTCRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(6))
	return cmd
}

func (lowTTCRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(6))
	return cmd
}

// Redis stub simulating missing impression timestamp for fail-closed tests.
type missingImpTSRedis struct {
	mockRedisClient
}

func (missingImpTSRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(7))
	return cmd
}

func (missingImpTSRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(7))
	return cmd
}

// Redis stub triggering TTC bypass path for metric tests.
type ttcBypassRedis struct {
	mockRedisClient
}

func (ttcBypassRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(10))
	return cmd
}

func (ttcBypassRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(10))
	return cmd
}
