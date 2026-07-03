package ads

import (
	"context"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
)

// fraudSignalsFilter injects stable fraud signals for engine benchmarks and SLA tests.
type fraudSignalsFilter struct {
	first  FraudReasonID
	second FraudReasonID
}

func (f *fraudSignalsFilter) Check(_ context.Context, evt *domain.Event) error {
	if f.first != FraudReasonNone {
		addFraudSignal(evt, f.first)
	}
	if f.second != FraudReasonNone {
		addFraudSignal(evt, f.second)
	}
	return nil
}

func benchFilterEngineFraudScoring(b *testing.B, filters ...EventFilter) (*FilterEngine, *domain.Event, context.Context) {
	b.Helper()
	engine := NewFilterEngine(0, filters...)
	engine.SetRegistry(&mockRegistry{})

	campID := uuid.New()
	cachedMockCamp.Store(&domain.Campaign{ID: campID})
	b.Cleanup(func() { cachedMockCamp.Store(nil) })

	evt := &domain.Event{
		CampaignID:   campID,
		StringBuffer: make([]byte, 0, 64),
	}
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		evt.ShadowEvent = false
		evt.FraudScore = 0
		evt.FraudReason = ""
		evt.StringBuffer = evt.StringBuffer[:0]
		_ = engine.Check(ctx, evt)
	}
	return engine, evt, ctx
}

func resetFraudBenchEvent(evt *domain.Event) {
	evt.ShadowEvent = false
	evt.FraudScore = 0
	evt.FraudReason = ""
	evt.StringBuffer = evt.StringBuffer[:0]
}

// Tracks FilterEngine.Check with fraud accumulator attached but no signals recorded.
func BenchmarkFilterEngine_Check_fraudScoring_noSignals(b *testing.B) {
	engine, evt, ctx := benchFilterEngineFraudScoring(b, &countingFilter{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	}
}

// Tracks FilterEngine.Check with one L2-weak signal and shadow decision.
func BenchmarkFilterEngine_Check_fraudScoring_L2Shadow(b *testing.B) {
	engine, evt, ctx := benchFilterEngineFraudScoring(b, &fraudSignalsFilter{first: FraudReasonMissingImpTS})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	}
}

// Tracks FilterEngine.Check with dual L1-high signals and L1 reject path.
func BenchmarkFilterEngine_Check_fraudScoring_L1Reject(b *testing.B) {
	engine, evt, ctx := benchFilterEngineFraudScoring(b, &fraudSignalsFilter{
		first:  FraudReasonDatacenterIP,
		second: FraudReasonLowTTC,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	}
}

func TestFilterEngine_Check_zeroAlloc_fraudScoring(t *testing.T) {
	cases := []struct {
		name    string
		filters []EventFilter
	}{
		{name: "no_signals", filters: []EventFilter{&countingFilter{}}},
		{name: "L2_shadow", filters: []EventFilter{&fraudSignalsFilter{first: FraudReasonMissingImpTS}}},
		{name: "L1_reject", filters: []EventFilter{&fraudSignalsFilter{
			first: FraudReasonDatacenterIP, second: FraudReasonLowTTC,
		}}},
	}
	ctx := context.Background()
	campID := uuid.New()
	cachedMockCamp.Store(&domain.Campaign{ID: campID})
	t.Cleanup(func() { cachedMockCamp.Store(nil) })

	evt := &domain.Event{
		CampaignID:   campID,
		StringBuffer: make([]byte, 0, 64),
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := NewFilterEngine(0, tc.filters...)
			engine.SetRegistry(&mockRegistry{})
			for i := 0; i < 100; i++ {
				resetFraudBenchEvent(evt)
				_ = engine.Check(ctx, evt)
			}
			avg := testing.AllocsPerRun(100, func() {
				resetFraudBenchEvent(evt)
				_ = engine.Check(ctx, evt)
			})
			if avg > 0 {
				t.Fatalf("FilterEngine.Check allocated %.1f times per run, want 0", avg)
			}
		})
	}
}

// Guards fraud scoring adds less than 500µs incremental cost per FilterEngine.Check.
func TestFilterEngine_FraudScoring_LatencySLA(t *testing.T) {
	const (
		iterations = 4000
		budget     = 500 * time.Microsecond
	)

	ctx := context.Background()
	campID := uuid.New()
	cachedMockCamp.Store(&domain.Campaign{ID: campID})
	t.Cleanup(func() { cachedMockCamp.Store(nil) })

	evt := &domain.Event{
		CampaignID:   campID,
		StringBuffer: make([]byte, 0, 64),
	}

	baseline := NewFilterEngine(0, &countingFilter{})
	scored := NewFilterEngine(0, &fraudSignalsFilter{
		first:  FraudReasonDatacenterIP,
		second: FraudReasonLowTTC,
	}, &countingFilter{})
	scored.SetRegistry(&mockRegistry{})

	for i := 0; i < 500; i++ {
		resetFraudBenchEvent(evt)
		_ = baseline.Check(ctx, evt)
		resetFraudBenchEvent(evt)
		_ = scored.Check(ctx, evt)
	}

	var baseTotal, scoredTotal int64
	for i := 0; i < iterations; i++ {
		resetFraudBenchEvent(evt)
		start := monotonicNano()
		_ = baseline.Check(ctx, evt)
		baseTotal += monotonicNano() - start

		resetFraudBenchEvent(evt)
		start = monotonicNano()
		_ = scored.Check(ctx, evt)
		scoredTotal += monotonicNano() - start
	}

	added := time.Duration((scoredTotal - baseTotal) / iterations)
	if added > budget {
		t.Fatalf("fraud scoring added %v per Check, budget %v (baseline %v scored %v over %d iters)",
			added, budget,
			time.Duration(baseTotal/iterations),
			time.Duration(scoredTotal/iterations),
			iterations)
	}
	t.Logf("fraud scoring incremental cost: %v (baseline %v, scored %v)",
		added,
		time.Duration(baseTotal/iterations),
		time.Duration(scoredTotal/iterations))
}
