package fraudscoring

import (
	"context"
	"testing"
	"time"
)

func BenchmarkLGBMScorer_ScoreBatch10k(b *testing.B) {
	if testing.Short() {
		b.Skip("skipped in -short; run manually: go test -bench=BenchmarkLGBMScorer_ScoreBatch10k -benchtime=1x ./internal/fraudscoring")
	}

	scorer, err := NewLGBMScorer("testdata/model.txt")
	if err != nil {
		b.Fatalf("load scorer: %v", err)
	}

	rows := make([]FeatureRow, 10_000)
	for i := range rows {
		rows[i] = FeatureRow{
			Events:           uint64(10 + i%50),
			Clicks:           uint64(i % 20),
			SpendMicro:       int64(1_000_000 + i),
			BudgetLimitMicro: 5_000_000,
			UniqueUsers:      1,
			UniqueUAs:        1,
		}
	}

	ctx := context.Background()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if _, err := scorer.ScoreBatch(ctx, rows); err != nil {
			b.Fatalf("score batch: %v", err)
		}
	}
	elapsed := time.Since(start)
	if b.N == 1 {
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/op10k")
		if elapsed > 2*time.Second {
			b.Fatalf("10k inference took %v, want < 2s", elapsed)
		}
	}
}

func TestLGBMScorer_ScoreBatch10k_under2s(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in -short; manual gate: go test -run TestLGBMScorer_ScoreBatch10k_under2s ./internal/fraudscoring")
	}

	scorer, err := NewLGBMScorer("testdata/model.txt")
	requireNoError(t, err)

	rows := make([]FeatureRow, 10_000)
	for i := range rows {
		rows[i] = FeatureRow{
			Events:           uint64(10 + i%50),
			Clicks:           uint64(i % 20),
			SpendMicro:       int64(1_000_000 + i),
			BudgetLimitMicro: 5_000_000,
			UniqueUsers:      1,
			UniqueUAs:        1,
		}
	}

	start := time.Now()
	_, err = scorer.ScoreBatch(context.Background(), rows)
	requireNoError(t, err)

	elapsed := time.Since(start)
	t.Logf("10k inference elapsed=%v", elapsed)
	if elapsed > 2*time.Second {
		t.Fatalf("10k inference took %v, want < 2s", elapsed)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
