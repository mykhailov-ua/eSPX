package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"espx/internal/config"
	"espx/internal/database"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_SellersJSONInvalid proves corrupt seller_type in DB yields HTTP 503, not malformed JSON to clients.
func TestChaos_SellersJSONInvalid(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	exportDir := t.TempDir()
	cfg := &config.Config{AdminAPIKey: "test-secret"}
	cfg.Management.SupplyExportPath = exportDir

	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()

	// Steady-state: valid seller returns 200 JSON.
	_, err := pool.Exec(ctx, `
		INSERT INTO sellers (seller_id, domain, seller_type, name)
		VALUES ('valid-1', 'good.example.com', 'PUBLISHER', 'Good Pub')`)
	require.NoError(t, err)

	req, _ := http.NewRequest("GET", "/.well-known/sellers.json", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	var baseline map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&baseline))
	require.NotEmpty(t, baseline["sellers"])

	// Fault: drop CHECK to simulate post-migration corruption, then inject invalid seller_type.
	_, err = pool.Exec(ctx, `ALTER TABLE sellers DROP CONSTRAINT sellers_seller_type_check`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE sellers SET seller_type = 'INVALID' WHERE seller_id = 'valid-1'`)
	require.NoError(t, err)
	invalidateSellersJSONCache()

	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusServiceUnavailable, resp.Code)
	assert.NotContains(t, resp.Body.String(), `"seller_type":"INVALID"`)

	logChaosProof(t, "sellers_json_invalid", map[string]string{
		"subsystem":      "management_supply",
		"baseline_ok":    "true",
		"fault_type":     "corrupt_seller_type",
		"http_status":    strconv.Itoa(resp.Code),
		"invalid_served": "false",
	})
}

// TestChaos_SupplyOutboxRedelivery proves concurrent outbox workers export supply files exactly once.
func TestChaos_SupplyOutboxRedelivery(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	exportDir := t.TempDir()
	cfg := &config.Config{AdminAPIKey: "test-secret"}
	cfg.Management.SupplyExportPath = exportDir

	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	ctx := context.Background()

	_, err := svc.CreateSeller(ctx, SellerCreateSpec{
		SellerID:   "redelivery-pub",
		Domain:     "redelivery.example.com",
		SellerType: "PUBLISHER",
		Name:       "Redelivery Test",
	})
	require.NoError(t, err)

	var eventID int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT id FROM outbox_events WHERE event_type = 'UPDATE_SUPPLY_FILES' ORDER BY id DESC LIMIT 1`,
	).Scan(&eventID))

	worker := NewOutboxWorker(svc)
	const workers = 4
	var wg sync.WaitGroup
	var totalProcessed atomic.Int32
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			n, err := worker.ProcessOutboxWithCount(ctx, 10)
			require.NoError(t, err)
			totalProcessed.Add(int32(n))
		}()
	}
	wg.Wait()

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&status))
	assert.Equal(t, "PROCESSED", status)
	assert.Equal(t, int32(1), totalProcessed.Load())

	sellersPath := filepath.Join(exportDir, "sellers.json")
	require.FileExists(t, sellersPath)
	raw, err := os.ReadFile(sellersPath)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	sellers := doc["sellers"].([]any)
	require.Len(t, sellers, 1)

	var processedCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox_events
		WHERE event_type = 'UPDATE_SUPPLY_FILES' AND status = 'PROCESSED' AND id = $1`, eventID,
	).Scan(&processedCount))
	assert.Equal(t, 1, processedCount)

	logChaosProof(t, "supply_outbox_redelivery", map[string]string{
		"subsystem":   "management_outbox",
		"workers":     "4",
		"processed":   "1",
		"baseline_ok": "true",
		"fault_type":  "concurrent_outbox_redelivery",
		"event_id":    strconv.FormatInt(eventID, 10),
	})
}
