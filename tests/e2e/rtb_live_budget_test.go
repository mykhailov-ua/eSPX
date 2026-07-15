package e2e_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/ingestion"
	"espx/internal/ingestion/pb"
	"espx/internal/ingestion/sqlc"
	"espx/internal/rtb"
	"espx/internal/testutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staticGeoCountry is a minimal GeoIP stub for RTB ingest geo deduplication in e2e.
type staticGeoCountry struct {
	country string
}

func (g staticGeoCountry) GetCountry(string) (string, error) { return g.country, nil }
func (g staticGeoCountry) IsAnonymous(string) (bool, error)  { return false, nil }
func (g staticGeoCountry) Close() error                      { return nil }

// TestE2E_RtbLiveBudgetAuthority exercises live RTB winner selection with RTB budget
// authority: Lua skip_budget preserves Redis keys; in-process BudgetStore debits spend.
func TestE2E_RtbLiveBudgetAuthority(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := testutil.SetupAdsPostgres(t)
	defer cleanupDB()

	rdb, cleanupRedis := testutil.SetupRedis(t)
	defer cleanupRedis()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	cfg := &config.Config{
		RtbMode:            "live",
		RtbBudgetAuthority: "rtb",
		ClickAmount:        100_000,
		EventBatchSize:     10,
		EventFlushMs:       100,
		MaxWorkers:         2,
		WriteTimeoutMs:     1000,
		FilterTimeoutMs:    1000,
		MaxRequestBodySize: 1024 * 1024,
		StreamMaxLen:       100000,
	}

	customerID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "RTB Customer", 1_000_000_000)
	require.NoError(t, err)

	campaignID := uuid.New()
	_, err = pool.Exec(ctx,
		`INSERT INTO campaigns (id, name, status, customer_id, budget_limit, target_countries)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		campaignID, "RTB Campaign", "ACTIVE", customerID, 100_000_000, []string{"US"},
	)
	require.NoError(t, err)

	registry := testutil.NewAdsRegistry(t, queries)
	_, err = registry.Sync(ctx)
	require.NoError(t, err)

	camp, ok := registry.GetCampaign(campaignID)
	require.True(t, ok)

	const redisBudgetMicro = int64(50_000_000)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, redisBudgetMicro, 0).Err())

	rtbStore := rtb.NewBudgetStore()
	catalog := ingestion.NewRtbCatalog(rtbStore, ingestion.BudgetAuthorityRTB)
	catalog.Registry().SetTargetingIndexEnabled(true)
	sharder := ingestion.NewJumpHashSharder(1)
	budgetSync := ingestion.RtbBudgetSync{
		Authority: ingestion.BudgetAuthorityRTB,
		Redis:     []redis.UniversalClient{rdb},
		Sharder:   sharder,
	}
	ingestion.SyncRtbCatalog(ctx, registry, catalog, cfg, nil, budgetSync)

	rtbCampID := ingestion.CampaignIDFromUUID(campaignID)
	rtbBudgetBefore := rtbStore.GetBudget(rtbCampID)
	require.Equal(t, redisBudgetMicro, rtbBudgetBefore)

	campaignRepo := ingestion.NewCampaignRepo(queries)
	unifiedFilter := ingestion.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		sharder,
		registry,
		campaignRepo,
		1000,
		time.Minute,
		45*time.Second,
		24*time.Hour,
		100_000,
		10_000,
		"rtb-e2e-stream",
		100000,
	)
	require.NoError(t, unifiedFilter.PreloadScripts(ctx))

	filterEngine := ingestion.NewFilterEngine(time.Duration(cfg.FilterTimeoutMs)*time.Millisecond, unifiedFilter)
	store := ingestion.NewPostgresStore(queries, 1*time.Second)
	consumer := ingestion.NewStreamConsumer(
		store, rdb, "rtb-e2e-stream", "rtb-e2e-group", "rtb-e2e-c1",
		cfg.EventBatchSize, cfg.MaxWorkers,
		100*time.Millisecond, 1*time.Second, 100*time.Millisecond,
		5*time.Second, 5, 5*time.Minute, 1*time.Second,
	)
	consumer.Start(ctx)
	defer consumer.Close()

	handler := ingestion.NewAdsPacketHandler(cfg, registry, filterEngine, pool, []redis.UniversalClient{rdb}, sharder, cfg.FraudStreamName, nil)
	handler.ConfigureIngestGeo(staticGeoCountry{country: "US"})
	handler.ConfigureRtb(catalog, staticGeoCountry{country: "US"}, unifiedFilter, nil)
	defer handler.Stop(ctx)

	clientCampID := uuid.New()
	pbEvt := &pb.AdEvent{
		CampaignId: clientCampID[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId:    []byte("rtb_e2e_click"),
			UserId:     []byte("rtb_e2e_user"),
			DeviceType: []byte("desktop"),
			ExtraBytes: []byte(`{"bid_micro":100000}`),
		},
	}
	body, err := pbEvt.MarshalVT()
	require.NoError(t, err)

	status, _ := ingestion.PostTrackGnet(handler, body, "application/x-protobuf", "application/x-protobuf")
	assert.Equal(t, http.StatusAccepted, status)

	redisAfter, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, redisBudgetMicro, redisAfter, "Lua skip_budget must leave Redis campaign budget unchanged")

	rtbBudgetAfter := rtbStore.GetBudget(rtbCampID)
	assert.Equal(t, rtbBudgetBefore-cfg.ClickAmount, rtbBudgetAfter, "RTB store must debit clearing price")

	assert.Eventually(t, func() bool {
		var clicks int64
		err = pool.QueryRow(ctx, "SELECT clicks_count FROM campaign_stats WHERE campaign_id = $1", campaignID).Scan(&clicks)
		return err == nil && clicks == 1
	}, 5*time.Second, 100*time.Millisecond, "stream consumer should persist click for RTB-selected campaign")
}
