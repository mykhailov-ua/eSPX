package ingestion

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"espx/internal/campaignmodel"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

type placementHExistsMock struct {
	mockRedisClient
	hit bool
}

func (m *placementHExistsMock) HExists(ctx context.Context, key string, field string) *redis.BoolCmd {
	staticBoolCmd.SetVal(m.hit)
	return staticBoolCmd
}

func setupPlacementBlacklistBench(t testing.TB, blacklisted bool) (*PlacementBlacklistFilter, *campaignmodel.Event, context.Context) {
	t.Helper()
	rdbs := []redis.UniversalClient{&placementHExistsMock{hit: blacklisted}}
	f := NewPlacementBlacklistFilter(rdbs)
	campID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	evt := &campaignmodel.Event{
		CampaignID:  campID,
		PlacementID: "zone-42",
	}
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = f.Check(ctx, evt)
	}
	return f, evt, ctx
}

// BenchmarkPlacementBlacklistFilter_miss measures hot-path cost when placement is allowed.
func BenchmarkPlacementBlacklistFilter_miss(b *testing.B) {
	f, evt, ctx := setupPlacementBlacklistBench(b, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

// BenchmarkPlacementBlacklistFilter_hit measures hot-path cost when placement is paused.
func BenchmarkPlacementBlacklistFilter_hit(b *testing.B) {
	f, evt, ctx := setupPlacementBlacklistBench(b, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

// TestPlacementBlacklistFilter_zeroAlloc guards the filter path stays allocation-free (mock Redis).
func TestPlacementBlacklistFilter_zeroAlloc(t *testing.T) {
	f, evt, ctx := setupPlacementBlacklistBench(t, false)
	avg := testing.AllocsPerRun(100, func() {
		_ = f.Check(ctx, evt)
	})
	if avg > 0 {
		t.Fatalf("PlacementBlacklistFilter.Check allocated %.1f times per run, want 0", avg)
	}
}

// TestPlacementBlacklistFilter_escapeClean verifies Check does not escape key buffer to heap.
func TestPlacementBlacklistFilter_escapeClean(t *testing.T) {
	if testing.Short() {
		t.Skip("escape analysis build")
	}
	root := moduleRootAds(t)
	cmd := exec.Command("go", "build", "-gcflags=-m", "-o", "/dev/null", "./internal/ingestion")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("escape analysis build failed: %v\n%s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, "PlacementBlacklistFilter).Check") {
			continue
		}
		if strings.Contains(line, "escapes to heap") {
			t.Fatalf("PlacementBlacklistFilter.Check escapes to heap: %s", strings.TrimSpace(line))
		}
	}
}
