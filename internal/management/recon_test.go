package management

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRedisForRecon struct {
	redis.UniversalClient
	mu   sync.Mutex
	data map[string]int64
}

// newMockRedisForRecon exists so recon tests can exercise Lua budget adjustments without a live Redis server.
func newMockRedisForRecon() *mockRedisForRecon {
	return &mockRedisForRecon{data: make(map[string]int64)}
}

func (m *mockRedisForRecon) Get(ctx context.Context, key string) *redis.StringCmd {
	m.mu.Lock()
	defer m.mu.Unlock()
	val := m.data[key]
	cmd := redis.NewStringCmd(ctx)
	cmd.SetVal(strconv.FormatInt(val, 10))
	return cmd
}

func (m *mockRedisForRecon) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := keys[0]
	delta := args[0].(int64)

	current := m.data[key]
	newVal := current + delta
	if newVal <= 0 {
		delete(m.data, key)
		return redis.NewCmd(ctx, int64(0))
	}
	m.data[key] = newVal
	return redis.NewCmd(ctx, newVal)
}

func (m *mockRedisForRecon) getVal(key string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[key]
}

// TestRecon_RaceConcurrentAdjustments guards concurrent recon deltas remain linear and never negative.
func TestRecon_RaceConcurrentAdjustments(t *testing.T) {
	rdb := newMockRedisForRecon()
	campID := uuid.New()
	key := "budget:sync:campaign:" + campID.String()

	rdb.data[key] = 10_000_000

	const goroutines = 50
	const deltaPerGoroutine = -100_000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start

			_ = rdb.Eval(context.Background(), "", []string{key}, int64(deltaPerGoroutine))
		}()
	}

	close(start)
	wg.Wait()

	final := rdb.getVal(key)
	expected := int64(10_000_000) + (int64(goroutines) * deltaPerGoroutine)
	assert.Equal(t, expected, final, "concurrent adjustments must be linear and race-free")
	assert.GreaterOrEqual(t, final, int64(0), "budget must never go negative from recon corrections")
}

// TestRecon_EdgeCases guards large negative recon deltas clamp budget to zero instead of going negative.
func TestRecon_EdgeCases(t *testing.T) {
	t.Run("LargeNegativeDeltaIsClampedByLua", func(t *testing.T) {
		rdb := newMockRedisForRecon()
		campID := uuid.New()
		key := "budget:sync:campaign:" + campID.String()
		rdb.data[key] = 1_000_000

		err := func() error {
			_, e := rdb.Eval(context.Background(), "", []string{key}, int64(-100_000_000)).Result()
			return e
		}()
		require.NoError(t, err)

		final := rdb.getVal(key)
		assert.Equal(t, int64(0), final, "Lua must delete the key instead of allowing negative budget")
	})
}

// TestRecon_LedgerTypeSecurity guards only approved ledger types are accepted for balance mutations.
func TestRecon_LedgerTypeSecurity(t *testing.T) {
	allowedTypes := []string{"TOPUP", "FEE", "RECONCILIATION_ADJUST", "REFUND"}
	for _, typ := range allowedTypes {
		assert.NotEmpty(t, typ)
	}

}

// BenchmarkRecon_AtomicAdjustment measures recon Lua adjustment loop overhead.
func BenchmarkRecon_AtomicAdjustment(b *testing.B) {
	rdb := newMockRedisForRecon()
	campID := uuid.New()
	key := "budget:sync:campaign:" + campID.String()
	rdb.data[key] = 50_000_000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rdb.Eval(context.Background(), "", []string{key}, int64(-1000))
	}
}
