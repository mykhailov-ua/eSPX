package testutil

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// SetupTestRedis starts an isolated Redis container for integration tests.
func SetupTestRedis(t testing.TB) (redis.UniversalClient, func()) {
	ctx := context.Background()
	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %s", err)
	}
	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %s", err)
	}
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{endpoint},
	})
	return rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

var (
	staticCmd       = redis.NewCmd(context.Background())
	staticStatusCmd = redis.NewStatusCmd(context.Background())
	staticStringCmd = redis.NewStringCmd(context.Background())
	staticBoolCmd   = redis.NewBoolCmd(context.Background())
)

// MockRedisClient is a Redis stub with pipeline and Lua hooks for filter tests.
type MockRedisClient struct {
	redis.UniversalClient
}

// MockPipeliner records commands for filter mocks.
type MockPipeliner struct {
	redis.Pipeliner
	IncrCmd  *redis.IntCmd
	DoCmd    *redis.Cmd
	XAddCmds []*redis.StringCmd
	XAddFail bool
}

func (m *MockPipeliner) Incr(ctx context.Context, key string) *redis.IntCmd {
	m.IncrCmd.SetVal(1)
	return m.IncrCmd
}

func (m *MockPipeliner) Do(ctx context.Context, args ...any) *redis.Cmd {
	return m.DoCmd
}

func (m *MockPipeliner) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	if m.XAddFail {
		cmd.SetErr(errors.New("fraud stream write failed"))
	} else {
		cmd.SetVal("1-0")
	}
	m.XAddCmds = append(m.XAddCmds, cmd)
	return cmd
}

func (m *MockPipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	for _, cmd := range m.XAddCmds {
		if err := cmd.Err(); err != nil {
			m.XAddCmds = m.XAddCmds[:0]
			return nil, err
		}
	}
	m.XAddCmds = m.XAddCmds[:0]
	return nil, nil
}

func (m *MockRedisClient) Pipeline() redis.Pipeliner {
	return &MockPipeliner{
		IncrCmd: redis.NewIntCmd(context.Background()),
		DoCmd:   redis.NewCmd(context.Background()),
	}
}

func (m *MockRedisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	return staticStatusCmd
}

func (m *MockRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	staticStringCmd.SetVal("1716223400000")
	return staticStringCmd
}

func (m *MockRedisClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	staticCmd.SetVal(int64(0))
	return staticCmd
}

func (m *MockRedisClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	staticCmd.SetVal(int64(0))
	return staticCmd
}

func (m *MockRedisClient) Process(ctx context.Context, cmd redis.Cmder) error {
	if c, ok := cmd.(*redis.Cmd); ok {
		c.SetVal(int64(0))
	}
	return nil
}

// SetProcessLuaInt64 sets a mock Lua integer result on a Redis command.
func SetProcessLuaInt64(cmd redis.Cmder, v int64) {
	if c, ok := cmd.(*redis.Cmd); ok {
		c.SetVal(v)
	}
}

// SetProcessLuaErr sets a mock Lua error on a Redis command.
func SetProcessLuaErr(cmd redis.Cmder, err error) {
	cmd.SetErr(err)
}

func (m *MockRedisClient) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	staticStringCmd.SetVal("d3b07384d113edec49eaa6238ad5ff00")
	return staticStringCmd
}

func (m *MockRedisClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	staticBoolCmd.SetVal(true)
	return staticBoolCmd
}

// MockRegistry is an in-memory campaign registry stub for handler and filter tests.
type MockRegistry struct{}

func (m *MockRegistry) Exists(id uuid.UUID) bool { return true }
func (m *MockRegistry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode domain.PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
}
func (m *MockRegistry) GetCustomerID(id uuid.UUID) (uuid.UUID, bool) { return uuid.Nil, true }

var (
	staticCampaignMu sync.RWMutex
	staticCampaign   = &domain.Campaign{CustomerID: uuid.Nil, Location: time.UTC}
	// CachedMockCamp overrides GetCampaign results in tests.
	CachedMockCamp atomic.Pointer[domain.Campaign]
)

func (m *MockRegistry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	if got := CachedMockCamp.Load(); got != nil && got.ID == id {
		return got, true
	}

	staticCampaignMu.RLock()
	defer staticCampaignMu.RUnlock()

	cp := *staticCampaign
	cp.ID = id
	idStr := id.String()
	custStr := cp.CustomerID.String()
	cp.IDStr = idStr
	cp.IDStrAny = idStr
	cp.CustomerIDStr = custStr
	cp.CustomerIDStrAny = custStr

	cp.BudgetCampaignKey = "budget:campaign:" + idStr
	cp.CampaignSyncKey = "budget:sync:campaign:" + idStr
	cp.CustomerSyncKey = "budget:sync:customer:" + custStr
	if cp.BrandFcapKey != "" {
		cp.FcapKeyPrefix = cp.BrandFcapKey + ":u:"
	} else {
		cp.FcapKeyPrefix = "fcap:c:" + idStr + ":u:"
	}
	cp.DailySpendKeyPrefix = "budget:daily_spent:campaign:" + idStr + ":"

	CachedMockCamp.Store(&cp)
	return &cp, true
}

func (m *MockRegistry) Sync(ctx context.Context) (int, error)                 { return 0, nil }
func (m *MockRegistry) StartSync(ctx context.Context, interval time.Duration) {}
func (m *MockRegistry) Wait(ctx context.Context) error                        { return nil }
