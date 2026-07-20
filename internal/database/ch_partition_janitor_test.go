package database

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPartitionOlderThan(t *testing.T) {
	t.Parallel()

	cutoff := 202506
	assert.True(t, partitionOlderThan("202401", cutoff))
	assert.False(t, partitionOlderThan("202607", cutoff))
	assert.False(t, partitionOlderThan("bad", cutoff))
}

func TestCHOffPeakUTC(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		hour  int
		start int
		end   int
		want  bool
	}{
		{"inside window", 3, 2, 6, true},
		{"before window", 1, 2, 6, false},
		{"at end exclusive", 6, 2, 6, false},
		{"wrap evening", 23, 22, 6, true},
		{"wrap morning", 5, 22, 6, true},
		{"wrap midday", 12, 22, 6, false},
		{"equal bounds disabled", 3, 4, 4, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := base.Add(time.Duration(tc.hour) * time.Hour)
			assert.Equal(t, tc.want, chOffPeakUTC(now, tc.start, tc.end))
		})
	}
}

func TestCHPartitionJanitor_EmergencyDropAlerter(t *testing.T) {
	t.Parallel()

	var alerted bool
	var gotTable, gotPart string
	var gotPct float64

	j := NewCHPartitionJanitor(nil, CHJanitorOptions{
		OnEmergencyDrop: func(table, partition string, diskPct float64) {
			alerted = true
			gotTable = table
			gotPart = partition
			gotPct = diskPct
		},
	})
	if j.onEmergencyDrop == nil {
		t.Fatal("expected emergency drop alerter")
	}
	j.onEmergencyDrop("impressions", "202401", 92.5)
	assert.True(t, alerted)
	assert.Equal(t, "impressions", gotTable)
	assert.Equal(t, "202401", gotPart)
	assert.InDelta(t, 92.5, gotPct, 0.01)
}
