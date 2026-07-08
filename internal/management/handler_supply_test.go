package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSupplyAPI_CRUDAndExport(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	exportDir := t.TempDir()
	cfg := &config.Config{
		AdminAPIKey:       "test-secret",
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	cfg.Management.SupplyExportPath = exportDir

	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO system_settings (key, value) VALUES
			('supply_owner_domain', 'owner.example.com'),
			('supply_manager_domain', 'manager.example.com')
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`)
	require.NoError(t, err)

	// Create seller
	sellerBody, _ := json.Marshal(SellerCreateSpec{
		SellerID:   "pub-001",
		Domain:     "publisher.example.com",
		SellerType: "PUBLISHER",
		Name:       "Example Publisher",
	})
	req, _ := http.NewRequest("POST", "/admin/supply/sellers", bytes.NewReader(sellerBody))
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code)

	var seller SellerDTO
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&seller))
	assert.Equal(t, "pub-001", seller.SellerID)

	// Create ads.txt entry
	adsBody, _ := json.Marshal(AdsTxtEntryCreateSpec{
		Domain:             "google.com",
		PublisherAccountID: "pub-12345",
		Relationship:       "RESELLER",
		CertAuthorityID:    "f08c47fec0942fa0",
	})
	req, _ = http.NewRequest("POST", "/admin/supply/ads-txt", bytes.NewReader(adsBody))
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code)

	// Process outbox → export files
	worker := NewOutboxWorker(svc)
	n, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 2, n, "seller + ads-txt each enqueue one outbox event")

	sellersPath := filepath.Join(exportDir, "sellers.json")
	adsPath := filepath.Join(exportDir, "ads.txt")
	require.FileExists(t, sellersPath)
	require.FileExists(t, adsPath)

	sellersRaw, err := os.ReadFile(sellersPath)
	require.NoError(t, err)
	var sellersDoc map[string]any
	require.NoError(t, json.Unmarshal(sellersRaw, &sellersDoc))
	assert.Equal(t, "1.0", sellersDoc["version"])
	sellersArr := sellersDoc["sellers"].([]any)
	require.Len(t, sellersArr, 1)

	adsRaw, err := os.ReadFile(adsPath)
	require.NoError(t, err)
	adsText := string(adsRaw)
	assert.Contains(t, adsText, "OWNERDOMAIN=owner.example.com")
	assert.Contains(t, adsText, "MANAGERDOMAIN=manager.example.com")
	assert.Contains(t, adsText, "google.com, pub-12345, RESELLER, f08c47fec0942fa0")

	// Public sellers.json endpoint
	req, _ = http.NewRequest("GET", "/.well-known/sellers.json", nil)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header().Get("Content-Type"))
	assert.Contains(t, resp.Header().Get("Cache-Control"), "max-age=60")

	// Admin ads.txt export
	req, _ = http.NewRequest("GET", "/admin/supply/ads.txt", nil)
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "text/plain; charset=utf-8", resp.Header().Get("Content-Type"))
	assert.Contains(t, resp.Body.String(), "OWNERDOMAIN=owner.example.com")

	// Campaign supply chain
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Supply Co", 200_000_000, "USD"))
	campID, err := svc.CreateCampaign(ctx, CampaignCreateSpec{
		CustomerID:     customerID,
		Name:           "Chain Camp",
		BudgetLimit:    10_000_000,
		PacingMode:     "ASAP",
		Timezone:       "UTC",
		IdempotencyKey: "supply-chain-test",
	})
	require.NoError(t, err)

	chainBody, _ := json.Marshal(map[string]any{
		"nodes": []SupplyChainNode{
			{ASI: "exchange.example.com", SID: "1234", HP: 1},
		},
	})
	req, _ = http.NewRequest("PUT", "/admin/campaigns/"+campID.String()+"/supply-chain", bytes.NewReader(chainBody))
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var auditCount int64
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM admin_audit_log WHERE action = 'UPDATE_CAMPAIGN_SUPPLY_CHAIN' AND target_id = $1`,
		campID).Scan(&auditCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), auditCount)
}

func TestSupplyAPI_RBAC(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:       "test-secret",
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	authMW, tokenMaker := integrationTestAuth(t, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMW, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	managerID := uuid.New()
	attachManager := func(req *http.Request) {
		token, err := tokenMaker.CreateToken(uuid.New(), uuid.New(), RoleManager, managerID, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	}

	body, _ := json.Marshal(SellerCreateSpec{
		SellerID: "x", Domain: "x.com", SellerType: "PUBLISHER",
	})
	req, _ := http.NewRequest("POST", "/admin/supply/sellers", bytes.NewReader(body))
	attachManager(req)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusForbidden, resp.Code)
}
