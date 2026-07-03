package perimeter

import (
	"context"
	"strconv"
	"testing"
	"time"

	"espx/internal/database"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const edgeBlacklistSyncInterval = 5 * time.Second

// TestChaos_EdgePhase1BlocksBlacklistedIP verifies phase-1 returns 403 without body read.
func TestChaos_EdgePhase1BlocksBlacklistedIP(t *testing.T) {
	if testing.Short() {
		t.Skip("edge chaos integration test")
	}

	ctx := context.Background()
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	const blockedIP = "203.0.113.50"
	require.NoError(t, rdb.SAdd(ctx, redisKeyBlacklistManual, blockedIP).Err())

	cache := NewBlacklistCache(defaultStaleSec)
	require.NoError(t, cache.SyncFromRedis(ctx, rdb))

	var metrics Metrics
	now := time.Now().Unix()

	outcome := cache.Phase1Check(blockedIP, now, &metrics)
	assert.Equal(t, Phase1Blocked403, outcome)
	assert.Equal(t, int64(1), metrics.BlockedIP)
	assert.Equal(t, int64(0), metrics.BodyRead, "phase-1 must not read body")
	assert.Equal(t, int64(0), metrics.Phase1Pass)

	// Legit IP on same cache generation still passes phase-1.
	legitOutcome := cache.Phase1Check("198.51.100.1", now, &metrics)
	assert.Equal(t, Phase1Pass, legitOutcome)
	assert.Equal(t, int64(1), metrics.Phase1Pass)

	logChaosProof(t, "edge_phase1_blacklist", map[string]string{
		"blocked_before_body": "true",
		"body_read_total":     strconv.FormatInt(metrics.BodyRead, 10),
		"blocked_ip_total":    strconv.FormatInt(metrics.BlockedIP, 10),
		"harness":             "go_perimeter_mirror",
	})
}

// TestChaos_EdgeBlacklistPropagation verifies Redis blacklist:manual reaches edge cache within 5s.
func TestChaos_EdgeBlacklistPropagation(t *testing.T) {
	if testing.Short() {
		t.Skip("edge chaos integration test")
	}

	ctx := context.Background()
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	const newIP = "198.51.100.77"
	cache := NewBlacklistCache(defaultStaleSec)
	require.NoError(t, cache.SyncFromRedis(ctx, rdb))

	now := time.Now().Unix()
	require.Equal(t, Phase1Pass, cache.Phase1Check(newIP, now, nil))

	addedAt := time.Now()
	require.NoError(t, rdb.SAdd(ctx, redisKeyBlacklistManual, newIP).Err())

	var blockedWithin time.Duration
	deadline := addedAt.Add(edgeBlacklistSyncInterval)
	var metrics Metrics

	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		require.NoError(t, cache.SyncFromRedis(ctx, rdb))
		now = time.Now().Unix()
		if cache.Phase1Check(newIP, now, &metrics) == Phase1Blocked403 {
			blockedWithin = time.Since(addedAt)
			break
		}
	}

	require.NotZero(t, blockedWithin, "edge must block IP within %s sync window", edgeBlacklistSyncInterval)
	assert.LessOrEqual(t, blockedWithin, edgeBlacklistSyncInterval)
	assert.Equal(t, int64(0), metrics.BodyRead, "blacklist block must occur before body read")
	assert.GreaterOrEqual(t, metrics.BlockedIP, int64(1))

	seconds := int(blockedWithin.Round(time.Second) / time.Second)
	if seconds < 1 {
		seconds = 1
	}

	logChaosProof(t, "edge_blacklist_propagation", map[string]string{
		"blocked_within_seconds": strconv.Itoa(seconds),
		"body_read_total":        strconv.FormatInt(metrics.BodyRead, 10),
		"sync_interval_sec":      strconv.Itoa(int(edgeBlacklistSyncInterval / time.Second)),
		"harness":                "go_perimeter_mirror",
	})
}

// TestChaos_EdgeFraudBlacklistPropagation verifies blacklist:fraud reaches edge cache within 5s.
func TestChaos_EdgeFraudBlacklistPropagation(t *testing.T) {
	if testing.Short() {
		t.Skip("edge chaos integration test")
	}

	ctx := context.Background()
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	const fraudIP = "203.0.113.99"
	cache := NewBlacklistCache(defaultStaleSec)
	require.NoError(t, cache.SyncFromRedis(ctx, rdb))

	now := time.Now().Unix()
	require.Equal(t, Phase1Pass, cache.Phase1Check(fraudIP, now, nil))

	addedAt := time.Now()
	require.NoError(t, rdb.SAdd(ctx, redisKeyBlacklistFraud, fraudIP).Err())

	var blockedWithin time.Duration
	deadline := addedAt.Add(edgeBlacklistSyncInterval)
	var metrics Metrics

	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		require.NoError(t, cache.SyncFromRedis(ctx, rdb))
		now = time.Now().Unix()
		if cache.Phase1Check(fraudIP, now, &metrics) == Phase1Blocked403 {
			blockedWithin = time.Since(addedAt)
			break
		}
	}

	require.NotZero(t, blockedWithin, "edge must block fraud IP within %s sync window", edgeBlacklistSyncInterval)
	assert.Equal(t, int64(0), metrics.BodyRead)

	logChaosProof(t, "edge_fraud_blacklist_propagation", map[string]string{
		"source":            "blacklist:fraud",
		"blocked_within_ms": strconv.FormatInt(blockedWithin.Milliseconds(), 10),
		"harness":           "go_perimeter_mirror",
	})
}

// TestChaos_ASNWhitelistBypass verifies CDN ASN bypasses phase-1 blacklist without body read.
func TestChaos_ASNWhitelistBypass(t *testing.T) {
	if testing.Short() {
		t.Skip("edge chaos integration test")
	}

	ctx := context.Background()
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	const blockedIP = "198.51.100.88"
	require.NoError(t, rdb.SAdd(ctx, redisKeyBlacklistManual, blockedIP).Err())

	cache := NewBlacklistCache(defaultStaleSec)
	cache.SetASNWhitelist(NewASNWhitelist("15169", ""))
	require.NoError(t, cache.SyncFromRedis(ctx, rdb))

	now := time.Now().Unix()
	var metrics Metrics

	outcome := cache.Phase1CheckASN(blockedIP, "15169", now, &metrics)
	assert.Equal(t, Phase1Pass, outcome)
	assert.Equal(t, int64(1), metrics.Phase1Pass)
	assert.Equal(t, int64(0), metrics.BlockedIP)
	assert.Equal(t, int64(0), metrics.BodyRead)

	logChaosProof(t, "edge_asn_whitelist_bypass", map[string]string{
		"asn":                 "15169",
		"blocked_ip_bypassed": "true",
		"body_read_total":     "0",
		"harness":             "go_perimeter_mirror",
	})
}

// TestChaos_FraudTierBlock verifies score >= 81 maps to immediate edge block tier.
func TestChaos_FraudTierBlock(t *testing.T) {
	tier, score := MapFraudRLTier(85)
	assert.Equal(t, FraudRLTierBlock, tier)
	assert.True(t, ShouldBlockTier(tier))
	assert.Equal(t, 0, TierLimit(tier, DefaultFraudRLConfig()))
	assert.Equal(t, 120, RetryAfterSec(tier, DefaultFraudRLConfig()))
	assert.Equal(t, 85, score)

	logChaosProof(t, "edge_fraud_tier_block", map[string]string{
		"fraud_score":     "85",
		"tier":            string(tier),
		"retry_after_sec": "120",
		"harness":         "go_perimeter_mirror",
	})
}
