package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"github.com/redis/go-redis/v9"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Provides isolated Redis for ads integration tests.
func setupTestRedis(t testing.TB) (redis.UniversalClient, func()) {
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

// Redis client stub with pipeline and Lua hooks for filter tests and benches.
type mockRedisClient struct {
	redis.UniversalClient
}

// Redis pipeline stub recording commands for filter mocks.
type mockPipeliner struct {
	redis.Pipeliner
	incrCmd  *redis.IntCmd
	doCmd    *redis.Cmd
	xaddCmds []*redis.StringCmd
	xaddFail bool
}

func (m *mockPipeliner) Incr(ctx context.Context, key string) *redis.IntCmd {
	m.incrCmd.SetVal(1)
	return m.incrCmd
}

func (m *mockPipeliner) Do(ctx context.Context, args ...any) *redis.Cmd {
	return m.doCmd
}

func (m *mockPipeliner) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	if m.xaddFail {
		cmd.SetErr(errors.New("fraud stream write failed"))
	} else {
		cmd.SetVal("1-0")
	}
	m.xaddCmds = append(m.xaddCmds, cmd)
	return cmd
}

func (m *mockPipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	for _, cmd := range m.xaddCmds {
		if err := cmd.Err(); err != nil {
			m.xaddCmds = m.xaddCmds[:0]
			return nil, err
		}
	}
	m.xaddCmds = m.xaddCmds[:0]
	return nil, nil
}

func (m *mockRedisClient) Pipeline() redis.Pipeliner {
	return &mockPipeliner{
		incrCmd: redis.NewIntCmd(context.Background()),
		doCmd:   redis.NewCmd(context.Background()),
	}
}

func (m *mockRedisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	return staticStatusCmd
}

func (m *mockRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	staticStringCmd.SetVal("1716223400000")
	return staticStringCmd
}

func (m *mockRedisClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	staticCmd.SetVal(int64(0))
	return staticCmd
}

func (m *mockRedisClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	staticCmd.SetVal(int64(0))
	return staticCmd
}

func (m *mockRedisClient) Process(ctx context.Context, cmd redis.Cmder) error {
	if c, ok := cmd.(*redis.Cmd); ok {
		c.SetVal(int64(0))
	}
	return nil
}

func setProcessLuaInt64(cmd redis.Cmder, v int64) {
	if c, ok := cmd.(*redis.Cmd); ok {
		c.SetVal(v)
	}
}

func setProcessLuaErr(cmd redis.Cmder, err error) {
	cmd.SetErr(err)
}

func (m *mockRedisClient) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	staticStringCmd.SetVal("d3b07384d113edec49eaa6238ad5ff00")
	return staticStringCmd
}

func (m *mockRedisClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	staticBoolCmd.SetVal(true)
	return staticBoolCmd
}

func (m *mockRedisClient) HExists(ctx context.Context, key string, field string) *redis.BoolCmd {
	staticBoolCmd.SetVal(false)
	return staticBoolCmd
}

type errFilter struct {
	err error
}

func (f *errFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	return f.err
}

type slowFilter struct {
	delay time.Duration
}

func (f *slowFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	delay := f.delay
	if rem, ok := filterDeadlineRemainingEvt(evt, ctx); ok && rem < delay {
		delay = rem
	}
	if delay <= 0 {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		if filterDeadlineExceededEvt(evt, ctx) {
			return context.DeadlineExceeded
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type countingFilter struct {
	calls int
	err   error
}

func (f *countingFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	f.calls++
	return f.err
}

type deadlineProbeFilter struct {
	seen *bool
}

func (f *deadlineProbeFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	if evt != nil && evt.FilterDeadlineMono > 0 {
		*f.seen = true
		return nil
	}
	_, ok := filterDeadlineMonoFromContext(ctx)
	*f.seen = ok
	return nil
}
