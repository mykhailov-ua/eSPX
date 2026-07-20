package ivtdetector

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalBotnetRule_Find_flagsTimerBot(t *testing.T) {
	if testing.Short() {
		t.Skip("clickhouse integration test")
	}

	conn, cleanup := setupClickHouseTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedIntervalBotClicks(t, conn, "203.0.113.50", "timer-bot-ua", 35, time.Second)

	rule := &intervalBotnetRule{
		q: database.NewCHQuery(conn, database.CHQueryConfig{}),
		cfg: AnalyzerConfig{
			Window:               time.Hour,
			IntervalMinIntervals: 30,
			IntervalMaxVariance:  0.005,
		},
	}

	found, err := rule.Find(ctx)
	require.NoError(t, err)

	var botIP string
	for _, candidate := range found {
		if candidate.IP == "203.0.113.50" {
			botIP = candidate.IP
			assert.Equal(t, intervalBotReason, candidate.Reason)
			assert.Less(t, candidate.Score, 0.005)
		}
	}
	assert.Equal(t, "203.0.113.50", botIP, "expected timer bot IP in suspicious set: %+v", found)
}

func TestIntervalBotnetRule_Find_skipsJitteredTraffic(t *testing.T) {
	if testing.Short() {
		t.Skip("clickhouse integration test")
	}

	conn, cleanup := setupClickHouseTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	deltas := []time.Duration{
		400 * time.Millisecond, 2800 * time.Millisecond, 15200 * time.Millisecond, 900 * time.Millisecond,
		7100 * time.Millisecond, 22000 * time.Millisecond, 1300 * time.Millisecond, 5600 * time.Millisecond,
		11400 * time.Millisecond, 700 * time.Millisecond, 3200 * time.Millisecond, 18500 * time.Millisecond,
		2100 * time.Millisecond, 9800 * time.Millisecond, 500 * time.Millisecond, 14000 * time.Millisecond,
		4400 * time.Millisecond, 6700 * time.Millisecond, 1900 * time.Millisecond, 12300 * time.Millisecond,
		600 * time.Millisecond, 8200 * time.Millisecond, 3700 * time.Millisecond, 16100 * time.Millisecond,
		2500 * time.Millisecond, 10000 * time.Millisecond, 1100 * time.Millisecond, 7900 * time.Millisecond,
		4800 * time.Millisecond, 13600 * time.Millisecond, 800 * time.Millisecond, 5100 * time.Millisecond,
		19400 * time.Millisecond, 2000 * time.Millisecond, 11700 * time.Millisecond,
	}
	seedJitteredClicks(t, conn, "198.51.100.77", "human-ua", deltas)

	rule := &intervalBotnetRule{
		q: database.NewCHQuery(conn, database.CHQueryConfig{}),
		cfg: AnalyzerConfig{
			Window:               2 * time.Hour,
			IntervalMinIntervals: 30,
			IntervalMaxVariance:  0.005,
		},
	}

	found, err := rule.Find(ctx)
	require.NoError(t, err)
	for _, candidate := range found {
		assert.NotEqual(t, "198.51.100.77", candidate.IP, "jittered traffic should not be flagged")
	}
}
