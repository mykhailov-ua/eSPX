package ivtdetector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/edge/fingerprint"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedClickWithTLS(t *testing.T, conn driver.Conn, ip, ua, tlsHash string) uuid.UUID {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	campaignID := uuid.New()
	now := time.Now().UTC()
	clickID := fmt.Sprintf("tls-%s-%s", ip, tlsHash)
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO ad_event_processor.clicks
		(click_id, campaign_id, ip_address, user_agent, tls_hash, payload, created_at)
		VALUES (?, ?, ?, ?, ?, '', ?)`,
		clickID, campaignID, ip, ua, tlsHash, now,
	))
	return campaignID
}

func TestTCPEdgeCorrelationRule_GhostOnImpersonation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	conn, cleanupCH := setupClickHouseTest(t)
	defer cleanupCH()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx := context.Background()
	ip := "203.0.113.55"
	chromeUA := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
	pythonJA3 := "37b37375c33a2e6a17b2b6400c436321"

	campaignID := seedClickWithTLS(t, conn, ip, chromeUA, pythonJA3)

	require.NoError(t, fingerprint.Record(ctx, rdb, fingerprint.Entry{
		IP:      ip,
		TCPHash: 0xcafebabe,
		SeenAt:  time.Now().UTC(),
	}))

	rule := &tcpEdgeCorrelationRule{
		q:   database.NewCHQuery(conn, database.CHQueryConfig{}),
		rdb: rdb,
		cfg: AnalyzerConfig{Window: time.Hour},
	}

	candidates, err := rule.Find(ctx)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	assert.Equal(t, ip, candidates[0].IP)
	assert.Equal(t, campaignID.String(), candidates[0].CampaignID)
	assert.Equal(t, "ivt_tcp_edge_correlation", candidates[0].Reason)
	assert.Equal(t, "ghost", candidates[0].Action)
	assert.Equal(t, float64(70), candidates[0].Score)
}

func TestTCPEdgeCorrelationRule_SkipsMatchingUAJA3(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	conn, cleanupCH := setupClickHouseTest(t)
	defer cleanupCH()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx := context.Background()
	ip := "203.0.113.56"
	chromeUA := "Mozilla/5.0 Chrome/120.0.0.0"
	chromeJA3 := "chrome-ja3-fingerprint"

	seedClickWithTLS(t, conn, ip, chromeUA, chromeJA3)
	require.NoError(t, fingerprint.Record(ctx, rdb, fingerprint.Entry{
		IP:      ip,
		TCPHash: 0x12345678,
		SeenAt:  time.Now().UTC(),
	}))

	rule := &tcpEdgeCorrelationRule{
		q:   database.NewCHQuery(conn, database.CHQueryConfig{}),
		rdb: rdb,
		cfg: AnalyzerConfig{Window: time.Hour},
	}

	candidates, err := rule.Find(ctx)
	require.NoError(t, err)
	assert.Empty(t, candidates)
}
