package ivtdetector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/ingestion"
	"espx/internal/management"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type managementServiceBlocker struct {
	svc *management.Service
}

func (b *managementServiceBlocker) BlockIP(ctx context.Context, ip string) error {
	return b.svc.BlockIP(ctx, ip, "fraud")
}

func (b *managementServiceBlocker) EnqueueFraudThreat(context.Context, string, string, string, float64, int32, int64) error {
	return fmt.Errorf("not implemented")
}

// Guards interval-bot autoblock respects allowlist for protected resolver IPs.
func TestChaos_ivtIntervalAutoblock(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	conn, cleanupCH := setupClickHouseTest(t)
	defer cleanupCH()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	protectedIP := "8.8.8.8"
	botIP := "203.0.113.50"
	seedIntervalBotClicks(t, conn, protectedIP, "timer-bot-protected", 35, time.Second)
	seedIntervalBotClicks(t, conn, botIP, "timer-bot-open", 35, time.Second)

	rule := &intervalBotnetRule{
		q: database.NewCHQuery(conn, database.CHQueryConfig{}),
		cfg: AnalyzerConfig{
			Window:               time.Hour,
			IntervalMinIntervals: 30,
			IntervalMaxVariance:  0.005,
		},
	}
	candidates, err := rule.Find(ctx)
	require.NoError(t, err)

	foundProtected := false
	foundBot := false
	for _, candidate := range candidates {
		switch candidate.IP {
		case protectedIP:
			foundProtected = true
		case botIP:
			foundBot = true
		}
	}
	require.True(t, foundProtected, "expected protected timer bot in candidates")
	require.True(t, foundBot, "expected open timer bot in candidates")

	svc := management.NewService(pool, []redis.UniversalClient{rdb}, ingestion.NewJumpHashSharder(1), nil)
	defer svc.Close()

	err = svc.BlockIP(ctx, protectedIP, "fraud")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "protected by allowlist")

	detector := NewDetector(
		stubFinder{ips: []SuspiciousIP{
			{IP: botIP, Reason: intervalBotReason, Score: 0.0},
		}},
		NewIdempotencyStore(pool),
		&managementServiceBlocker{svc: svc},
		pool,
		DetectorConfig{OutboxPendingLimit: 0},
	)

	result, err := detector.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Enqueued)

	var protectedBlacklist int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM ip_blacklist WHERE ip = $1", protectedIP).Scan(&protectedBlacklist)
	require.NoError(t, err)
	assert.Equal(t, 0, protectedBlacklist)

	var botBlacklist int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM ip_blacklist WHERE ip = $1", botIP).Scan(&botBlacklist)
	require.NoError(t, err)
	assert.Equal(t, 1, botBlacklist)

	logChaosProof(t, "ivt_interval_autoblock", map[string]string{
		"subsystem":           "ivt_detector",
		"allowlist_respected": "true",
		"protected_ip":        protectedIP,
		"blocked_ip":          botIP,
	})
}
