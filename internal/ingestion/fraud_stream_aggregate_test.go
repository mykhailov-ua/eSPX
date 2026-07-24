package ingestion

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ipv4Subnet24Prefix(t *testing.T) {
	prefix, ok := ipv4Subnet24Prefix("203.0.113.45")
	require.True(t, ok)
	assert.Equal(t, uint32(0xCB007100), prefix)
}

func Test_fraudAggregateExempt_L3(t *testing.T) {
	evt := &campaignmodel.Event{FraudReason: FraudReasonCodeL3Blocklist}
	assert.True(t, fraudAggregateExempt(evt))
}

func Test_fraudAggregateExempt_dualL1(t *testing.T) {
	evt := &campaignmodel.Event{
		FraudReason: FraudReasonCodeDatacenterIP + "," + FraudReasonCodeLowTTC,
	}
	assert.True(t, fraudAggregateExempt(evt))
}

func Test_fraudAggregateExempt_singleL2(t *testing.T) {
	evt := &campaignmodel.Event{FraudReason: FraudReasonCodeMissingImpTS}
	assert.False(t, fraudAggregateExempt(evt))
}

func primeFraudRingAggPressure(q *FraudStreamWriter) {
	q.readCursor = fraudRingUsable - fraudAggThreshold
	q.allocCursor = fraudRingUsable
	q.writeCursor = fraudRingUsable
}

func primeFraudRingPending() uint64 {
	return fraudAggThreshold
}

func TestFraudStreamWriter_aggregateModeAtThreshold(t *testing.T) {
	q := &FraudStreamWriter{stopCh: make(chan struct{})}
	q.allocCursor = fraudAggThreshold + 100
	q.readCursor = 100

	q.publishAggMode(0)
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.FraudStreamMode.WithLabelValues("aggregating")))

	fill := q.ringFill()
	q.publishAggMode(fill)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.FraudStreamMode.WithLabelValues("aggregating")))
	assert.EqualValues(t, fraudAggThreshold, fill)
}

func TestFraudStreamWriter_aggregateIncrementsWithoutRing(t *testing.T) {
	q := &FraudStreamWriter{stopCh: make(chan struct{})}
	primeFraudRingAggPressure(q)

	beforeAgg := testutil.ToFloat64(metrics.FraudStreamAggregatedTotal)
	evt := &campaignmodel.Event{
		IP:          "10.20.30.40",
		FraudReason: FraudReasonCodeLowTTC,
		Type:        "click",
	}
	require.True(t, q.Enqueue(0, evt))
	assert.Equal(t, beforeAgg+1, testutil.ToFloat64(metrics.FraudStreamAggregatedTotal))
	assert.Equal(t, q.Pending(), primeFraudRingPending())
}

func TestFraudStreamWriter_L3NeverAggregated(t *testing.T) {
	q := &FraudStreamWriter{
		stream: "fraud-stream",
		maxLen: 1000,
		rdbs:   []redis.UniversalClient{&mockRedisClient{}},
		stopCh: make(chan struct{}),
	}
	primeFraudRingAggPressure(q)

	evt := &campaignmodel.Event{
		ClickID:     "l3-click",
		CampaignID:  uuid.New(),
		IP:          "10.0.0.1",
		FraudReason: FraudReasonCodeL3Blocklist,
		Type:        "click",
	}
	require.True(t, q.Enqueue(0, evt))
	assert.Equal(t, primeFraudRingPending()+1, q.Pending())
}

func TestChaos_FraudStreamL3NeverAggregated(t *testing.T) {
	q := &FraudStreamWriter{
		stream: "fraud-stream-chaos",
		maxLen: 1000,
		rdbs:   []redis.UniversalClient{&mockRedisClient{}},
		stopCh: make(chan struct{}),
	}
	primeFraudRingAggPressure(q)

	beforePending := q.Pending()
	beforeAgg := testutil.ToFloat64(metrics.FraudStreamAggregatedTotal)

	l3 := &campaignmodel.Event{
		ClickID:     "l3",
		CampaignID:  uuid.New(),
		IP:          "198.18.1.1",
		FraudReason: FraudReasonCodeL3Blocklist,
		Type:        "click",
	}
	l2 := &campaignmodel.Event{
		IP:          "198.18.1.2",
		FraudReason: FraudReasonCodeMissingImpTS,
		Type:        "click",
	}

	require.True(t, q.Enqueue(0, l3))
	require.True(t, q.Enqueue(0, l2))

	assert.Equal(t, beforePending+1, q.Pending())
	assert.Equal(t, beforeAgg+1, testutil.ToFloat64(metrics.FraudStreamAggregatedTotal))
	logChaosProof(t, "fraud_stream_l3_never_aggregated", map[string]string{
		"ring_pending": "1",
		"l3_enqueued":  "true",
	})
}

func TestChaos_FraudStreamCriticalLaneAnalyticalFull(t *testing.T) {
	q := &FraudStreamWriter{
		stream: "fraud-crit",
		maxLen: 1000,
		rdbs:   []redis.UniversalClient{&mockRedisClient{}},
		stopCh: make(chan struct{}),
	}
	// Analytical ring at 100% usable fill.
	q.readCursor = 0
	q.allocCursor = fraudAnalyticalUsable
	q.writeCursor = fraudAnalyticalUsable

	beforeCritDrop := testutil.ToFloat64(metrics.FraudStreamCriticalDropTotal)
	beforeAgg := testutil.ToFloat64(metrics.FraudStreamAggregatedTotal)

	l3 := &campaignmodel.Event{
		ClickID:     "crit-l3",
		CampaignID:  uuid.New(),
		IP:          "203.0.113.9",
		FraudReason: FraudReasonCodeL3Blocklist,
		Type:        "click",
	}
	require.True(t, q.Enqueue(0, l3))
	assert.Equal(t, beforeCritDrop, testutil.ToFloat64(metrics.FraudStreamCriticalDropTotal))
	assert.Equal(t, beforeAgg, testutil.ToFloat64(metrics.FraudStreamAggregatedTotal))
	assert.Equal(t, uint64(1), pendingDelta(atomic.LoadUint64(&q.critWrite), atomic.LoadUint64(&q.critRead)))

	logChaosProof(t, "fraud_critical_lane_no_agg", map[string]string{
		"analytical_fill": "100",
		"l3_critical":     "true",
	})
}

func TestFraudStreamWriter_SetForceAggregate(t *testing.T) {
	q := &FraudStreamWriter{stopCh: make(chan struct{})}
	before := testutil.ToFloat64(metrics.FraudStreamBackpressureTotal)
	q.SetForceAggregate(true)
	assert.Equal(t, uint32(1), atomic.LoadUint32(&q.forceAgg))
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.FraudStreamBackpressureTotal))
	q.SetForceAggregate(false)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&q.forceAgg))
}

func TestFraudStreamWriter_aggregateFlushToStream(t *testing.T) {
	rdb := &capturingRedisXAdd{}
	q := &FraudStreamWriter{
		stream: "fraud-stream",
		maxLen: 1000,
		rdbs:   []redis.UniversalClient{rdb},
		stopCh: make(chan struct{}),
	}
	primeFraudRingAggPressure(q)

	evt := &campaignmodel.Event{
		IP:          "192.168.10.20",
		FraudReason: FraudReasonCodeLowTTC,
	}
	require.True(t, q.Enqueue(0, evt))

	q.flushAggregates(true)
	require.NotEmpty(t, rdb.lastArgs)
	assert.Equal(t, "fraud_aggregate", rdb.lastArgs["type"])
	assert.Equal(t, "192.168.10.0/24", rdb.lastArgs["subnet"])
	assert.Equal(t, FraudReasonCodeLowTTC, rdb.lastArgs["fraud_reason"])
	assert.Equal(t, "1", rdb.lastArgs["count"])
}

func TestFraudStreamWriter_spike50kZeroRingDrops(t *testing.T) {
	q := &FraudStreamWriter{stopCh: make(chan struct{})}
	primeFraudRingAggPressure(q)

	beforeRingDrop := testutil.ToFloat64(metrics.FraudStreamDropTotal)
	beforeAgg := testutil.ToFloat64(metrics.FraudStreamAggregatedTotal)
	evt := &campaignmodel.Event{
		IP:          "10.1.2.3",
		FraudReason: FraudReasonCodeMissingImpTS,
		Type:        "click",
	}

	const producers = 32
	const perProducer = 1600
	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				q.Enqueue(0, evt)
			}
		}()
	}
	wg.Wait()

	afterRingDrop := testutil.ToFloat64(metrics.FraudStreamDropTotal)
	assert.Equal(t, beforeRingDrop, afterRingDrop)
	assert.Greater(t, testutil.ToFloat64(metrics.FraudStreamAggregatedTotal)-beforeAgg, float64(50000))
}

func TestFraudStreamWriter_aggregateTableOverflowIncrementsDropped(t *testing.T) {
	q := &FraudStreamWriter{stopCh: make(chan struct{})}
	for i := range q.aggSlots {
		cell := &q.aggSlots[i]
		cell.subnetPrefix.Store(uint32(0xC0000000 + uint32(i<<8)))
		cell.reasonID.Store(uint32(FraudReasonLowTTC))
		cell.count.Store(1)
	}
	atomic.StoreUint64(&q.aggOccupied, fraudAggTableSize)
	metrics.FraudStreamAggTableFill.Set(1)

	before := testutil.ToFloat64(metrics.FraudStreamAggregatedDropTotal)
	evt := &campaignmodel.Event{
		IP:          "10.99.1.1",
		FraudReason: FraudReasonCodeLowTTC,
	}
	require.False(t, q.aggregateEvent(evt))
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.FraudStreamAggregatedDropTotal))
}

func TestStreamConsumer_parseFraudAggregate(t *testing.T) {
	consumer := &StreamConsumer{}
	evt := consumer.parseMessage("1-0", map[string]interface{}{
		"type":         "fraud_aggregate",
		"subnet":       "10.0.0.0/24",
		"fraud_reason": "low_ttc",
		"count":        "1500",
		"window_ms":    "75",
	})
	require.NotNil(t, evt)
	assert.Equal(t, fraudAggregateEventType, evt.Type)
	assert.Equal(t, "10.0.0.0/24", evt.IP)
	assert.Equal(t, "low_ttc", evt.FraudReason)
	assert.Equal(t, "1500", evt.ClickID)
	assert.Equal(t, "75", evt.UserID)
	count, window := fraudAggregateFields(evt)
	assert.Equal(t, uint64(1500), count)
	assert.Equal(t, uint32(75), window)
}

type capturingRedisXAdd struct {
	mockRedisClient
	lastArgs map[string]string
}

func (m *capturingRedisXAdd) Pipeline() redis.Pipeliner {
	parent := m
	return &capturingPipeliner{parent: parent}
}

type capturingPipeliner struct {
	mockPipeliner
	parent *capturingRedisXAdd
}

func (p *capturingPipeliner) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	switch vals := args.Values.(type) {
	case []any:
		p.parent.lastArgs = valuesToMap(vals)
	case map[string]interface{}:
		m := make(map[string]string, len(vals))
		for k, v := range vals {
			m[k], _ = v.(string)
		}
		p.parent.lastArgs = m
	case map[string]string:
		p.parent.lastArgs = vals
	}
	return p.mockPipeliner.XAdd(ctx, args)
}

func valuesToMap(vals []any) map[string]string {
	m := make(map[string]string, len(vals)/2)
	for i := 0; i+1 < len(vals); i += 2 {
		k, _ := vals[i].(string)
		v, _ := vals[i+1].(string)
		m[k] = v
	}
	return m
}

func BenchmarkFraudAggregate(b *testing.B) {
	q := &FraudStreamWriter{stopCh: make(chan struct{})}
	atomic.StoreUint32(&q.aggregating, 1)

	evt := &campaignmodel.Event{
		IP:          "203.0.113.10",
		FraudReason: FraudReasonCodeLowTTC,
		Type:        "click",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.aggregateEvent(evt)
	}
}

func TestFraudAggregate_ZeroAlloc(t *testing.T) {
	q := &FraudStreamWriter{stopCh: make(chan struct{})}
	atomic.StoreUint32(&q.aggregating, 1)
	evt := &campaignmodel.Event{
		IP:          "203.0.113.10",
		FraudReason: FraudReasonCodeLowTTC,
	}
	allocs := testing.AllocsPerRun(1000, func() {
		q.aggregateEvent(evt)
	})
	if allocs != 0 {
		t.Fatalf("aggregateEvent allocs/op = %v, want 0", allocs)
	}
}
