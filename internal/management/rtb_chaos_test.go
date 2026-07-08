package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/rtb"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_rtb_catalog_reload_outbox proves admin deal mutation propagates PG→outbox→Redis→deal index reload.
func TestChaos_rtb_catalog_reload_outbox(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Chaos RTB", 1_000_000, "USD"))

	body, _ := json.Marshal(RtbDealCreateSpec{
		DealID:     "chaos-reload-deal",
		FloorMicro: 100_000,
		CustomerID: customerID.String(),
	})
	req, _ := http.NewRequest("POST", "/admin/rtb/deals", bytes.NewReader(body))
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code)

	var created RtbDealDTO
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))

	catalog := ads.NewRtbCatalog(rtb.NewBudgetStore(), ads.BudgetAuthorityShadow)
	require.NoError(t, ads.ReloadRtbDeals(ctx, db.New(pool), catalog))
	deal, ok := catalog.LookupDeal("chaos-reload-deal")
	require.True(t, ok)
	require.Equal(t, int64(100_000), deal.FloorMicro)

	channel := ads.RtbCatalogReloadChannel(svc.cfg)
	sub := rdb.Subscribe(ctx, channel)
	defer sub.Close()
	_, err := sub.Receive(ctx)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)

	updateBody, _ := json.Marshal(RtbDealUpdateSpec{
		DealID:     "chaos-reload-deal",
		FloorMicro: 275_000,
		CustomerID: customerID.String(),
	})
	req, _ = http.NewRequest("PUT", "/admin/rtb/deals/"+strconv.FormatInt(created.ID, 10), bytes.NewReader(updateBody))
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	const workers = 24
	var lookups atomic.Uint64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if d, found := catalog.LookupDeal("chaos-reload-deal"); found {
						if d.FloorMicro == 100_000 || d.FloorMicro == 275_000 {
							lookups.Add(1)
						}
					}
				}
			}
		}()
	}

	worker := NewOutboxWorker(svc)
	n, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	msg, err := sub.ReceiveTimeout(ctx, 2*time.Second)
	require.NoError(t, err)
	m, ok := msg.(*redis.Message)
	require.True(t, ok)
	assert.Equal(t, "reload", m.Payload)

	require.NoError(t, ads.ReloadRtbDeals(ctx, db.New(pool), catalog))
	close(stop)
	wg.Wait()

	reloaded, ok := catalog.LookupDeal("chaos-reload-deal")
	require.True(t, ok)
	assert.Equal(t, int64(275_000), reloaded.FloorMicro)
	assert.Greater(t, lookups.Load(), uint64(0))

	logChaosProof(t, "rtb_catalog_reload_outbox", map[string]string{
		"subsystem":       "management_rtb",
		"baseline_ok":     "true",
		"fault_type":      "deal_update_outbox_redis",
		"workers":         "24",
		"floor_before":    "100000",
		"floor_after":     "275000",
		"pubsub_received": "true",
		"lookups":         strconv.FormatUint(lookups.Load(), 10),
	})
}
