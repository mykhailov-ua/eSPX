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
	"espx/internal/testutil"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_ReportsDashboardsViews_TierGates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping reports dashboards views integration test")
	}

	ctx := context.Background()

	// 1. Setup Postgres
	cfgPostgres := testutil.DefaultPostgresConfig()
	cfgPostgres.MigrationDirs = []string{testutil.AdsMigrationsDir(), testutil.BillingMigrationsDir()}
	dbPool, cleanupDB := testutil.SetupPostgres(t, cfgPostgres)
	defer cleanupDB()

	// 2. Insert Subscription Plans (basic, pro, enterprise)
	limitsRaw := `{"max_active_campaigns": 50, "max_rps": 10000, "max_requests_per_day": 500000, "max_events_per_month": 10000, "max_regions": 1, "max_api_keys": 2, "max_export_chunk_bytes": 1048576, "quota_reset_timezone": "UTC"}`
	featuresRaw := `{"rtb_live": false, "ml_fraud_boost": false, "multi_region": false, "slot_migration": false}`

	_, err := dbPool.Exec(ctx, `
		INSERT INTO billing.subscription_plans (code, display_name, limits_json, features_json, base_fee_micro)
		VALUES 
			('basic', 'Basic Plan', $1, $2, 10000000),
			('pro', 'Pro Plan', $1, $2, 50000000),
			('enterprise', 'Enterprise Plan', $1, $2, 200000000)
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
	customerEnterprise := uuid.New()

	customers := []struct {
		id   uuid.UUID
		name string
		plan string
	}{
		{customerBasic, "Basic Customer", "basic"},
		{customerPro, "Pro Customer", "pro"},
		{customerEnterprise, "Enterprise Customer", "enterprise"},
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

	// 4. Set up Handlers with the real dbPool
	reportsHandler := &adminapi.ReportsHTTPHandlers{
		Pool: dbPool,
		ResolveForecastCustomerID: func(r *http.Request, bodyID *uuid.UUID) (*uuid.UUID, error) {
			// Resolve customer ID from query param for testing
			if custIDStr := r.URL.Query().Get("customer_id"); custIDStr != "" {
				parsed, err := uuid.Parse(custIDStr)
				if err == nil {
					return &parsed, nil
				}
			}
			return nil, nil
		},
	}

	viewsHandler := &adminapi.ViewsHTTPHandlers{
		Service: adminapi.NewService(),
		Pool:    dbPool,
	}

	licensingHandler := &adminapi.LicensingHTTPHandlers{
		Pool: dbPool,
		ResolveSelfServeCustomerID: func(r *http.Request) (uuid.UUID, error) {
			if custIDStr := r.URL.Query().Get("customer_id"); custIDStr != "" {
				return uuid.Parse(custIDStr)
			}
			return uuid.Nil, nil
		},
		RequireSelfServePermission: func(perm string, next http.HandlerFunc) http.HandlerFunc {
			return next
		},
	}

	mux := http.NewServeMux()
	reportsHandler.Register(mux)
	viewsHandler.Register(mux)
	licensingHandler.Register(mux)

	// 5. Run tests for GET /api/v1/reports/placements
	t.Run("Reports_Placements_TierGate", func(t *testing.T) {
		// Basic plan -> 403 Forbidden
		req := httptest.NewRequest("GET", "/api/v1/reports/placements?customer_id="+customerBasic.String(), nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)

		// Pro plan -> 200 OK
		req = httptest.NewRequest("GET", "/api/v1/reports/placements?customer_id="+customerPro.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Enterprise plan -> 200 OK
		req = httptest.NewRequest("GET", "/api/v1/reports/placements?customer_id="+customerEnterprise.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	// 6. Run tests for GET /api/v1/reports/keywords
	t.Run("Reports_Keywords_TierGate", func(t *testing.T) {
		// Basic plan -> 403 Forbidden
		req := httptest.NewRequest("GET", "/api/v1/reports/keywords?customer_id="+customerBasic.String(), nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)

		// Pro plan -> 200 OK
		req = httptest.NewRequest("GET", "/api/v1/reports/keywords?customer_id="+customerPro.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Enterprise plan -> 200 OK
		req = httptest.NewRequest("GET", "/api/v1/reports/keywords?customer_id="+customerEnterprise.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	// 7. Run tests for Views (Enterprise only)
	t.Run("Views_CRUD_TierGate", func(t *testing.T) {
		// Basic plan -> 403 Forbidden on Create
		createReq := adminapi.CreateViewRequest{
			CustomerID: customerBasic.String(),
			Name:       "Basic View",
			ReportKey:  "placements",
			Spec:       map[string]any{},
		}
		body, _ := json.Marshal(createReq)
		req := httptest.NewRequest("POST", "/api/v1/views", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)

		// Pro plan -> 403 Forbidden on Create
		createReq.CustomerID = customerPro.String()
		body, _ = json.Marshal(createReq)
		req = httptest.NewRequest("POST", "/api/v1/views", bytes.NewReader(body))
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)

		// Enterprise plan -> 201 Created on Create
		createReq.CustomerID = customerEnterprise.String()
		body, _ = json.Marshal(createReq)
		req = httptest.NewRequest("POST", "/api/v1/views", bytes.NewReader(body))
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusCreated, w.Code)

		var created adminapi.SavedViewDTO
		err = json.Unmarshal(w.Body.Bytes(), &created)
		require.NoError(t, err)

		// Basic plan -> 403 Forbidden on List
		req = httptest.NewRequest("GET", "/api/v1/views?customer_id="+customerBasic.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)

		// Pro plan -> 403 Forbidden on List
		req = httptest.NewRequest("GET", "/api/v1/views?customer_id="+customerPro.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)

		// Enterprise plan -> 200 OK on List
		req = httptest.NewRequest("GET", "/api/v1/views?customer_id="+customerEnterprise.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	// 8. Run tests for GET /api/v1/selfserve/usage
	t.Run("SelfServe_Usage_TierGate", func(t *testing.T) {
		// Basic plan -> 403 Forbidden
		req := httptest.NewRequest("GET", "/api/v1/selfserve/usage?customer_id="+customerBasic.String(), nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)

		// Pro plan -> 200 OK (returns empty list because no meters inserted, but code is 200)
		req = httptest.NewRequest("GET", "/api/v1/selfserve/usage?customer_id="+customerPro.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Enterprise plan -> 200 OK
		req = httptest.NewRequest("GET", "/api/v1/selfserve/usage?customer_id="+customerEnterprise.String(), nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}
