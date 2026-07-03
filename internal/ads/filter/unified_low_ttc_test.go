package filter

import (
	"context"
	"strconv"
	"testing"
	"time"

	"espx/internal/ads/sharding"
	"espx/internal/domain"
	"espx/internal/metrics"

	adstest "espx/internal/ads/testutil"
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
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	f := NewTestUnifiedFilter(t, rdb)
	f.SetTTCMin(300 * time.Millisecond)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	SeedTestCampaignBudget(t, ctx, rdb, campID)

	impKey := "imp_ts:user1:" + campID.String()
	require.NoError(t, rdb.Set(ctx, impKey, strconv.FormatInt(time.Now().Add(-50*time.Millisecond).UnixMilli(), 10), 10*time.Minute).Err())

	evt := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: campID,
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}

	err := checkWithFraudScoring(ctx, f, evt)
	require.NoError(t, err)
	assert.True(t, evt.ShadowEvent)
	assert.Equal(t, FraudReasonCodeLowTTC, evt.FraudReason)
}

// Guards impression sets imp timestamp that click TTC check consumes.
func TestUnifiedFilter_impressionSetsImpTS_clickChecksTTC(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	f := NewTestUnifiedFilter(t, rdb)
	f.SetTTCMin(50 * time.Millisecond)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	SeedTestCampaignBudget(t, ctx, rdb, campID)

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
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	f := NewTestUnifiedFilter(t, rdb)
	f.SetTTCMin(300 * time.Millisecond)
	f.SetTTCFailClosed(true)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	SeedTestCampaignBudget(t, ctx, rdb, campID)

	click := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: campID,
		IP:         "1.1.1.1",
		ClickID:    uuid.NewString(),
	}
	err := checkWithFraudScoring(ctx, f, click)
	require.NoError(t, err)
	assert.True(t, click.ShadowEvent)
	assert.Equal(t, FraudReasonCodeMissingImpTS, click.FraudReason)
}

// Guards low TTC rejection path with mock Redis Lua responses.
func TestUnifiedFilter_lowTTC_mockRedis(t *testing.T) {
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&lowTTCRedis{}},
		sharding.NewJumpHashSharder(1),
		&adstest.MockRegistry{},
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
		Type:         "click",
		UserID:       "user1",
		CampaignID:   uuid.New(),
		IP:           "1.1.1.1",
		ClickID:      uuid.NewString(),
		StringBuffer: make([]byte, 0, 32),
	}
	err := checkWithFraudScoring(context.Background(), f, evt)
	require.NoError(t, err)
	assert.True(t, evt.ShadowEvent)
	assert.Equal(t, FraudReasonCodeLowTTC, evt.FraudReason)
}

// Guards missing impression timestamp fails closed with mock Redis.
func TestUnifiedFilter_failClosed_missingImpTS_mockRedis(t *testing.T) {
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&missingImpTSRedis{}},
		sharding.NewJumpHashSharder(1),
		&adstest.MockRegistry{},
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
		Type:         "click",
		UserID:       "user1",
		CampaignID:   uuid.New(),
		IP:           "1.1.1.1",
		ClickID:      uuid.NewString(),
		StringBuffer: make([]byte, 0, 32),
	}
	err := checkWithFraudScoring(context.Background(), f, evt)
	require.NoError(t, err)
	assert.True(t, evt.ShadowEvent)
	assert.Equal(t, FraudReasonCodeMissingImpTS, evt.FraudReason)
}

// Guards TTC bypass path increments telemetry when impression check skipped.
func TestUnifiedFilter_ttcBypass_incrementsMetric(t *testing.T) {
	before := testutil.ToFloat64(metrics.TTCBypassTotal)
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&ttcBypassRedis{}},
		sharding.NewJumpHashSharder(1),
		&adstest.MockRegistry{},
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
	adstest.MockRedisClient
}

func (lowTTCRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(6))
	return cmd
}

func (lowTTCRedis) Process(ctx context.Context, cmd redis.Cmder) error {
	adstest.SetProcessLuaInt64(cmd, 6)
	return nil
}

func (lowTTCRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(6))
	return cmd
}

// Redis stub simulating missing impression timestamp for fail-closed tests.
type missingImpTSRedis struct {
	adstest.MockRedisClient
}

func (missingImpTSRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(7))
	return cmd
}

func (missingImpTSRedis) Process(ctx context.Context, cmd redis.Cmder) error {
	adstest.SetProcessLuaInt64(cmd, 7)
	return nil
}

func (missingImpTSRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(7))
	return cmd
}

// Redis stub triggering TTC bypass path for metric tests.
type ttcBypassRedis struct {
	adstest.MockRedisClient
}

func (ttcBypassRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(10))
	return cmd
}

func (ttcBypassRedis) Process(ctx context.Context, cmd redis.Cmder) error {
	adstest.SetProcessLuaInt64(cmd, 10)
	return nil
}

func (ttcBypassRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(10))
	return cmd
}

// checkWithFraudScoring runs one filter through FilterEngine so fraud signals finalize on the event.
func checkWithFraudScoring(ctx context.Context, f EventFilter, evt *domain.Event) error {
	engine := NewFilterEngine(time.Second, f)
	engine.SetRegistry(&adstest.MockRegistry{})
	return engine.Check(ctx, evt)
}
