package costsync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func BenchmarkIngestKey(b *testing.B) {
	customerID := uuid.New()
	campaignID := uuid.New()
	date := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = IngestKey(customerID, campaignID, date, "facebook", "ad-1", LineTypeSpend)
	}
}

func BenchmarkCurrencyEURToUSD(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ConvertEURToUSD(100 * microUnit)
	}
}

func BenchmarkPersistLines(b *testing.B) {
	pool := setupCostSyncDB(b)
	ctx := context.Background()
	customerID, campaignID := seedCustomerCampaign(b, pool)
	worker := NewWorker(pool, []byte("postback-encryption-secret-key32"))
	lines := []CostLine{{
		CustomerID:  customerID,
		CampaignID:  campaignID,
		Date:        time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Network:     "facebook",
		PlacementID: "ad-bench",
		LineType:    LineTypeSpend,
		AmountMicro: 1_000_000,
		Currency:    "USD",
	}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lines[0].PlacementID = "ad-bench-" + string(rune('a'+i%26))
		_, _, _ = worker.persistLines(ctx, lines, lines[0].Date)
	}
}
