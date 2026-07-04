package ads

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type spyObserver struct {
	mu    sync.Mutex
	calls []float64
}

func (s *spyObserver) Observe(v float64) {
	s.mu.Lock()
	s.calls = append(s.calls, v)
	s.mu.Unlock()
}

func (s *spyObserver) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func TestNewRedisLuaObservers_length(t *testing.T) {
	observers := newRedisLuaObservers(4)
	require.Len(t, observers, 4)
	for _, o := range observers {
		require.NotNil(t, o)
	}
}

func TestNewRedisLuaObservers_nonPositiveDefaultsToOne(t *testing.T) {
	observers := newRedisLuaObservers(0)
	require.Len(t, observers, 1)
}

func TestObserveRedisLua_preboundShard(t *testing.T) {
	t.Parallel()
	spy := &spyObserver{}
	observers := []prometheus.Observer{spy, &spyObserver{}}

	observeRedisLua(observers, 0, 0.123)

	require.Equal(t, 1, spy.count())
}

func TestObserveRedisLua_outOfRangeUsesFallback(t *testing.T) {
	t.Parallel()
	observers := newRedisLuaObservers(2)

	require.NotPanics(t, func() {
		observeRedisLua(observers, 99, 0.05)
	})
}

func TestObserveRedisLua_parallel(t *testing.T) {
	t.Parallel()
	observers := newRedisLuaObservers(8)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			observeRedisLua(observers, shard%8, 0.001)
		}(i)
	}
	wg.Wait()
}

func TestPreboundTrackMetrics_countersDistinct(t *testing.T) {
	pm := newPreboundTrackMetrics()

	beforeAccepted := testutil.ToFloat64(pm.decisionAccepted)
	beforeProto := testutil.ToFloat64(pm.throughputProto)

	pm.decisionAccepted.Inc()
	pm.throughputProto.Inc()

	afterAccepted := testutil.ToFloat64(pm.decisionAccepted)
	afterProto := testutil.ToFloat64(pm.throughputProto)

	assert.Equal(t, beforeAccepted+1, afterAccepted)
	assert.Equal(t, beforeProto+1, afterProto)
}

func TestPreboundTrackMetrics_rejectPairs(t *testing.T) {
	pm := newPreboundTrackMetrics()

	beforeBlocked := testutil.ToFloat64(pm.blockedDuplicate)
	beforeDecision := testutil.ToFloat64(pm.decisionDuplicate)

	pm.blockedDuplicate.Inc()
	pm.decisionDuplicate.Inc()

	afterBlocked := testutil.ToFloat64(pm.blockedDuplicate)
	afterDecision := testutil.ToFloat64(pm.decisionDuplicate)

	assert.Equal(t, beforeBlocked+1, afterBlocked)
	assert.Equal(t, beforeDecision+1, afterDecision)
}

func TestPreboundTrackMetrics_concurrentInc(t *testing.T) {
	pm := newPreboundTrackMetrics()
	const n = 200

	before := testutil.ToFloat64(pm.decisionAccepted)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pm.decisionAccepted.Inc()
		}()
	}
	wg.Wait()

	after := testutil.ToFloat64(pm.decisionAccepted)
	assert.Equal(t, before+float64(n), after)
}
