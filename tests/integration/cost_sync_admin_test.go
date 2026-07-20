package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/adminapi"
	"espx/internal/costsync"
	"espx/internal/testutil"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCostSyncAdminAPIIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	cfg := testutil.DefaultPostgresConfig()
	cfg.MigrationDirs = []string{testutil.AdsMigrationsDir(), testutil.BillingMigrationsDir()}
	pool, cleanup := testutil.SetupPostgres(t, cfg)
	defer cleanup()

	customerID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'pro', 0, 'USD')`, customerID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO billing.customer_subscriptions (customer_id, plan_code, status, period_start)
		VALUES ($1, 'pro', 'active', $2)`, customerID, time.Now().Add(-time.Hour))
	require.NoError(t, err)

	key := []byte("postback-encryption-secret-key32")
	mem := &costsync.MemorySnapshotInserter{}
	worker := costsync.NewWorker(pool, key, costsync.WithMemorySnapshots(mem))

	handler := &adminapi.CostSyncHTTPHandlers{
		Pool:          pool,
		EncryptionKey: key,
		Worker:        worker,
	}
	mux := http.NewServeMux()
	handler.Register(mux)

	upsertBody, _ := json.Marshal(adminapi.UpsertCostSyncCredentialRequest{
		CustomerID:   customerID.String(),
		AccountID:    "act_test",
		AccessToken:  "token",
		RefreshToken: "refresh",
	})
	req := httptest.NewRequest(http.MethodPut, "/api/v1/cost-sync/credentials/facebook", bytes.NewReader(upsertBody))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/cost-sync/credentials?customer_id="+customerID.String(), nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	runBody, _ := json.Marshal(adminapi.RunCostSyncRequest{
		CustomerID: customerID.String(),
		Network:    "facebook",
		From:       "2026-07-01",
		To:         "2026-07-01",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/v1/cost-sync/run", bytes.NewReader(runBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/cost-sync/history?customer_id="+customerID.String(), nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}
