package ingestion

import (
	"context"
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/licensing"
)

func BenchmarkFilterLicense(b *testing.B) {
	f := NewLicenseFilter(&stubLicenseRegistry{state: licensing.StateActive})
	evt := &campaignmodel.Event{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

func TestFilterLicense_zeroAllocs(t *testing.T) {
	f := NewLicenseFilter(&stubLicenseRegistry{state: licensing.StateActive})
	evt := &campaignmodel.Event{}
	ctx := context.Background()

	allocs := testing.AllocsPerRun(1000, func() {
		_ = f.Check(ctx, evt)
	})
	if allocs > 0 {
		t.Fatalf("LicenseFilter.Check allocated %.1f times per run, want 0", allocs)
	}
}
