package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestE2EFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := repository.New(pool)
	cfg := &config.Config{
		EventBatchSize: 10,
		EventFlushMs:   100,
		StatsFlushMs:   100,
		MaxWorkers:     2,
		WriteTimeoutMs: 1000,
	}

	partManager := database.NewPartitionManager(pool, 7, 2)
	err := partManager.Run(ctx)
	require.NoError(t, err)

	campaignID := uuid.New()
	_, err = pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)", campaignID, "E2E Campaign", "active")
	require.NoError(t, err)

	registry := ads.NewRegistry(queries)
	_, _ = registry.Sync(ctx)

	eventProc := ads.NewProcessor(queries, cfg.EventBatchSize, cfg.MaxWorkers, 100*time.Millisecond, 1*time.Second)
	eventProc.Start(ctx)
	defer eventProc.Close()

	statsAgg := ads.NewAggregator(queries, 100*time.Millisecond, 1*time.Second, cfg.MaxWorkers)
	statsAgg.Start(ctx)

	router := ads.NewRouter(cfg, registry, eventProc, statsAgg)
	srv := httptest.NewServer(router)
	defer srv.Close()

	payload := map[string]any{
		"campaign_id": campaignID,
		"type":        "click",
		"payload":     map[string]string{"foo": "bar"},
	}
	body, _ := json.Marshal(payload)

	resp, err := http.Post(srv.URL+"/track", "application/json", bytes.NewBuffer(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)

	time.Sleep(500 * time.Millisecond)

	var clicks int64
	err = pool.QueryRow(ctx, "SELECT clicks_count FROM campaign_stats WHERE campaign_id = $1", campaignID).Scan(&clicks)
	require.NoError(t, err)
	assert.Equal(t, int64(1), clicks)

	var eventCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE campaign_id = $1", campaignID).Scan(&eventCount)
	require.NoError(t, err)
	assert.Equal(t, 1, eventCount)
}

func TestE2EFlow_Protobuf(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := repository.New(pool)
	cfg := &config.Config{
		EventBatchSize: 10,
		EventFlushMs:   100,
		StatsFlushMs:   100,
		MaxWorkers:     2,
	}

	campaignID := uuid.New()
	_, _ = pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)", campaignID, "Proto Campaign", "active")

	registry := ads.NewRegistry(queries)
	_, _ = registry.Sync(ctx)

	eventProc := ads.NewProcessor(queries, cfg.EventBatchSize, cfg.MaxWorkers, 100*time.Millisecond, 1*time.Second)
	eventProc.Start(ctx)
	defer eventProc.Close()

	statsAgg := ads.NewAggregator(queries, 100*time.Millisecond, 1*time.Second, cfg.MaxWorkers)
	statsAgg.Start(ctx)

	router := ads.NewRouter(cfg, registry, eventProc, statsAgg)
	srv := httptest.NewServer(router)
	defer srv.Close()

	pbEvt := &pb.AdEvent{
		CampaignId: campaignID.String(),
		EventType:  "impression",
		Metadata: &pb.EventMetadata{
			ClickId:    "click_123",
			UserId:     "user_456",
			DeviceType: "mobile",
			Os:         "android",
		},
	}
	body, _ := proto.Marshal(pbEvt)

	req, _ := http.NewRequest("POST", srv.URL+"/track", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.Equal(t, "application/x-protobuf", resp.Header.Get("Content-Type"))

	// Verify response body
	respBody, _ := io.ReadAll(resp.Body)
	var pbResp pb.TrackResponse
	err = proto.Unmarshal(respBody, &pbResp)
	require.NoError(t, err)
	assert.NotEmpty(t, pbResp.RequestId)
	assert.Equal(t, "accepted", pbResp.Status)

	time.Sleep(500 * time.Millisecond)

	var imps int64
	err = pool.QueryRow(ctx, "SELECT impressions_count FROM campaign_stats WHERE campaign_id = $1", campaignID).Scan(&imps)
	require.NoError(t, err)
	assert.Equal(t, int64(1), imps)
}
