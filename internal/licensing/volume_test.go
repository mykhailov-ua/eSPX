package licensing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWeightedBillableUnits_goldenFixture(t *testing.T) {
	counts := map[BillableCategory]uint64{
		BillableAccepted:    1000,
		BillableDedupReject: 500,
		BillableEbpfDrop:    2000,
	}
	got := WeightedBillableUnits(counts)
	assert.Equal(t, float64(1050), got)
}

func TestClassifyEventType(t *testing.T) {
	assert.Equal(t, BillableAccepted, ClassifyEventType("click"))
	assert.Equal(t, BillableDedupReject, ClassifyEventType("duplicate"))
	assert.Equal(t, BillableEbpfDrop, ClassifyEventType("ebpf_drop"))
}

func TestMonthlyPU(t *testing.T) {
	features := FeatureSet{
		OpenRTBEngine: true,
		IvtMLDetector: true,
	}
	pu := MonthlyPU(VolumeBandMedium, features)
	assert.Equal(t, 250+120+80, pu)
}

func TestDetermineState_graceContinues(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	claims := &LicenseClaims{
		ValidFrom:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ValidUntil: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		GraceDays:  14,
	}
	assert.Equal(t, StateGrace, DetermineState(claims, now, false))
}

func TestFeatureSet_OpenRTBBackwardCompat(t *testing.T) {
	f := FeatureSet{RtbLive: true}
	assert.True(t, f.OpenRTBEnabled())
}
