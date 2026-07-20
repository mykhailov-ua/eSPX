package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"espx/internal/adminapi"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/testutil"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestPostbacksAdminAPIIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// 1. Setup DB
	ctx := context.Background()
	cfgPostgres := testutil.DefaultPostgresConfig()
	cfgPostgres.MigrationDirs = []string{testutil.AdsMigrationsDir(), testutil.BillingMigrationsDir()}
	dbPool, cleanupDB := testutil.SetupPostgres(t, cfgPostgres)
	defer cleanupDB()

	// 2. Insert Subscription Plans (basic, pro)
	limitsRaw := `{"max_active_campaigns": 50, "max_rps": 10000, "max_requests_per_day": 500000, "max_events_per_month": 10000, "max_regions": 1, "max_api_keys": 2, "max_export_chunk_bytes": 1048576, "quota_reset_timezone": "UTC"}`
	featuresRaw := `{"rtb_live": false, "ml_fraud_boost": false, "multi_region": false, "slot_migration": false}`

	_, err := dbPool.Exec(ctx, `
		INSERT INTO billing.subscription_plans (code, display_name, limits_json, features_json, base_fee_micro)
		VALUES 
			('basic', 'Basic Plan', $1, $2, 10000000),
			('pro', 'Pro Plan', $1, $2, 50000000)
		ON CONFLICT (code) DO UPDATE SET
			display_name = EXCLUDED.display_name,
			limits_json = EXCLUDED.limits_json,
			features_json = EXCLUDED.features_json,
			base_fee_micro = EXCLUDED.base_fee_micro
	`, []byte(limitsRaw), []byte(featuresRaw))
	require.NoError(t, err)

	// 3. Create Customers and active Subscriptions
	customerBasic := uuid.New()
	customerPro := uuid.New()

	customers := []struct {
		id   uuid.UUID
		name string
		plan string
	}{
		{customerBasic, "Basic Customer", "basic"},
		{customerPro, "Pro Customer", "pro"},
	}

	for _, c := range customers {
		_, err = dbPool.Exec(ctx, "INSERT INTO customers (id, name, balance, currency) VALUES ($1, $2, 0, 'USD')", c.id, c.name)
		require.NoError(t, err)

		_, err = dbPool.Exec(ctx, `
			INSERT INTO billing.customer_subscriptions (customer_id, plan_code, status, period_start)
			VALUES ($1, $2, 'active', $3)
		`, c.id, c.plan, time.Now().Add(-2*time.Hour))
		require.NoError(t, err)
	}

	// 4. Seed campaigns linked to customers
	campaignBasicID := uuid.New()
	_, err = dbPool.Exec(ctx, `
		INSERT INTO campaigns (id, name, status, customer_id)
		VALUES ($1, 'Basic Campaign', 'ACTIVE', $2)`,
		campaignBasicID,
		customerBasic,
	)
	require.NoError(t, err)

	campaignProID := uuid.New()
	_, err = dbPool.Exec(ctx, `
		INSERT INTO campaigns (id, name, status, customer_id)
		VALUES ($1, 'Pro Campaign', 'ACTIVE', $2)`,
		campaignProID,
		customerPro,
	)
	require.NoError(t, err)

	// 5. Setup Postback Admin Handlers
	key := []byte("postback-encryption-secret-key32")
	handler := &adminapi.PostbackHTTPHandlers{
		Pool:          dbPool,
		EncryptionKey: key,
	}

	mux := http.NewServeMux()
	handler.Register(mux)

	// 6. Test PUT /api/v1/postbacks/config/{campaign_id} for basic plan -> Expect 403 Forbidden
	configReq := adminapi.UpdatePostbackConfigRequest{
		Provider:    "facebook",
		UrlTemplate: "https://mock.com",
		ApiToken:    "token123",
		TargetEvent: "conversion",
	}
	bodyBytes, err := json.Marshal(configReq)
	require.NoError(t, err)

	req := httptest.NewRequest("PUT", "/api/v1/postbacks/config/"+campaignBasicID.String(), bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	// 7. Test PUT /api/v1/postbacks/config/{campaign_id} for pro plan -> Expect 200 OK
	req = httptest.NewRequest("PUT", "/api/v1/postbacks/config/"+campaignProID.String(), bytes.NewReader(bodyBytes))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify database entry
	q := db.New(dbPool)
	configEntry, err := q.GetPostbackConfig(ctx, pgtype.UUID{Bytes: campaignProID, Valid: true})
	require.NoError(t, err)
	require.Equal(t, "facebook", configEntry.Provider)
	require.Equal(t, "https://mock.com", configEntry.UrlTemplate)

	// 8. Test GET /api/v1/postbacks/config (lists all configs)
	req = httptest.NewRequest("GET", "/api/v1/postbacks/config", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var configs []adminapi.PostbackConfigDTO
	err = json.NewDecoder(rec.Body).Decode(&configs)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	require.Equal(t, campaignProID.String(), configs[0].CampaignID)

	// 9. Test GET /api/v1/postbacks/dlq
	// Let's seed a DLQ item first
	_, err = q.InsertPostbackDLQ(ctx, db.InsertPostbackDLQParams{
		OutboxEventID: 1001,
		CampaignID:    pgtype.UUID{Bytes: campaignProID, Valid: true},
		ClickID:       "click_dlq_abc",
		EventType:     "conversion",
		Payload:       []byte(`{"click_id": "click_dlq_abc"}`),
		FailuresCount: 5,
		LastError:     pgtype.Text{String: "timeout error", Valid: true},
		Status:        "FAILED",
	})
	require.NoError(t, err)

	req = httptest.NewRequest("GET", "/api/v1/postbacks/dlq", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var dlqs []adminapi.PostbackDlqDTO
	err = json.NewDecoder(rec.Body).Decode(&dlqs)
	require.NoError(t, err)
	require.Len(t, dlqs, 1)
	require.Equal(t, "click_dlq_abc", dlqs[0].ClickID)
	require.Equal(t, "FAILED", dlqs[0].Status)

	// 10. Test POST /api/v1/postbacks/dlq/{id}/retry
	dlqIDStr := strconv.FormatInt(dlqs[0].ID, 10)
	req = httptest.NewRequest("POST", "/api/v1/postbacks/dlq/"+dlqIDStr+"/retry", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify DLQ status updated to RETRIED
	dlqUpdated, err := q.GetPostbackDLQ(ctx, dlqs[0].ID)
	require.NoError(t, err)
	require.Equal(t, "RETRIED", dlqUpdated.Status)

	// Verify a SEND_POSTBACK outbox event was created
	outboxEvents, err := q.GetPendingPostbackEventsForUpdate(ctx, 10)
	require.NoError(t, err)
	require.Len(t, outboxEvents, 1)
	require.Equal(t, "SEND_POSTBACK", outboxEvents[0].EventType)
}
