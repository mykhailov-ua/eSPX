package ads

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"espx/internal/config"
	"espx/internal/domain"

	"github.com/google/uuid"
)

func setupFilterMLBoostBench(t testing.TB) (*FilterEngine, *domain.Event, context.Context) {
	t.Helper()
	cfg := &config.Config{}
	sw := NewSettingsWatcher(nil, cfg)
	campID := uuid.New()
	sw.mlBoosts.Store(&MLBoostSnapshot{
		Boosts: map[uuid.UUID]uint8{campID: 15},
	})

	engine := NewFilterEngine(0, &fraudSignalsFilter{first: FraudReasonMissingImpTS})
	engine.SetRegistry(&mockRegistry{})
	engine.SetSettingsWatcher(sw)

	cachedMockCamp.Store(&domain.Campaign{ID: campID})
	t.Cleanup(func() { cachedMockCamp.Store(nil) })

	evt := &domain.Event{
		CampaignID:   campID,
		StringBuffer: make([]byte, 0, 64),
	}
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	}
	return engine, evt, ctx
}

// BenchmarkFilterMLBoost measures the hot-path cost of ML score boost application.
func BenchmarkFilterMLBoost(b *testing.B) {
	engine, evt, ctx := setupFilterMLBoostBench(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	}
}

// TestFilterMLBoost_zeroAlloc guards the boost apply path stays allocation-free.
func TestFilterMLBoost_zeroAlloc(t *testing.T) {
	engine, evt, ctx := setupFilterMLBoostBench(t)
	for i := 0; i < 100; i++ {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	}
	avg := testing.AllocsPerRun(100, func() {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	})
	if avg > 0 {
		t.Fatalf("FilterEngine.Check with ML boost allocated %.1f times per run, want 0", avg)
	}
}

func moduleRootAds(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// TestFilterMLBoost_escapeClean verifies applyFraudLayerDecision boost path does not escape to heap.
func TestFilterMLBoost_escapeClean(t *testing.T) {
	if testing.Short() {
		t.Skip("escape analysis build")
	}
	root := moduleRootAds(t)
	cmd := exec.Command("go", "build", "-gcflags=-m", "-o", osDevNull(), "./internal/ads")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("escape analysis build failed: %v\n%s", err, out)
	}
	text := string(out)
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, "applyFraudLayerDecision") {
			continue
		}
		if strings.Contains(line, "escapes to heap") {
			t.Fatalf("applyFraudLayerDecision escapes to heap: %s", strings.TrimSpace(line))
		}
	}
}

func osDevNull() string {
	return "/dev/null"
}
