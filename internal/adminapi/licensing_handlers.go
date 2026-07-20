package adminapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"espx/internal/billing/db"

	"espx/internal/ingestion"
	ingestdb "espx/internal/ingestion/sqlc"
	lic "espx/internal/licensing"

	"espx/pkg/httpresponse"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/redis/go-redis/v9"
)

// LicensingHTTPHandlers serves Milestone 3 licensing and subscription JSON routes.
type LicensingHTTPHandlers struct {
	Pool              *pgxpool.Pool
	RedisForCustomer  func(uuid.UUID) redis.UniversalClient
	ApplyRateLimit    func(http.HandlerFunc) http.HandlerFunc
	RequirePermission func(string, http.HandlerFunc) http.HandlerFunc

	ApplySelfServeRateLimit    func(http.HandlerFunc) http.HandlerFunc
	RequireSelfServePermission func(string, http.HandlerFunc) http.HandlerFunc
	ResolveSelfServeCustomerID func(*http.Request) (uuid.UUID, error)
}

// Register mounts M3 licensing/subscription routes on mux.
func (h *LicensingHTTPHandlers) Register(mux *http.ServeMux) {
	if h == nil || h.Pool == nil {
		return
	}
	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	if limit == nil {
		limit = func(next http.HandlerFunc) http.HandlerFunc { return next }
	}
	if perm == nil {
		perm = func(_ string, next http.HandlerFunc) http.HandlerFunc { return next }
	}

	mux.HandleFunc("GET /api/v1/customers/{id}/subscription", limit(perm("customers:read", h.getCustomerSubscription)))
	mux.HandleFunc("GET /api/v1/customers/{id}/usage", limit(perm("customers:read", h.getCustomerUsage)))
	mux.HandleFunc("GET /api/v1/customers/{id}/usage/daily", limit(perm("customers:read", h.getCustomerUsageDaily)))
	mux.HandleFunc("GET /api/v1/customers/{id}/quota-status", limit(perm("customers:read", h.getCustomerQuotaStatus)))
	mux.HandleFunc("GET /api/v1/license/status", limit(perm("customers:read", h.getLicenseStatus)))
	mux.HandleFunc("POST /admin/customers/{id}/subscription", limit(perm("customers:write", h.postCustomerSubscription)))
	mux.HandleFunc("POST /admin/customers/{id}/quota-bump", limit(perm("customers:write", h.postCustomerQuotaBump)))

	if h.RequireSelfServePermission != nil && h.ResolveSelfServeCustomerID != nil {
		ssLimit := h.ApplySelfServeRateLimit
		if ssLimit == nil {
			ssLimit = limit
		}
		mux.HandleFunc("GET /api/v1/selfserve/usage", ssLimit(h.RequireSelfServePermission("customers:read", h.getSelfServeUsage)))
	}
}

func (h *LicensingHTTPHandlers) getCustomerSubscription(w http.ResponseWriter, r *http.Request) {
	custID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	q := db.New(h.Pool)
	sub, err := q.GetCustomerSubscription(r.Context(), ingestion.ToUUID(custID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	licEnt := defaultLicenseEntitlements()
	if licRow, err := q.GetLicenseStatus(r.Context()); err == nil {
		_ = json.Unmarshal(licRow.EntitlementsJson, &licEnt)
	}

	var limits lic.Limits
	_ = json.Unmarshal(sub.LimitsJson, &limits)
	var features lic.FeatureSet
	_ = json.Unmarshal(sub.FeaturesJson, &features)
	applySubscriptionOverrides(sub.OverridesJson, &limits, &features)

	eff := lic.Effective(licEnt, lic.Entitlements{Limits: limits, Features: features})

	periodDate := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	meters, _ := q.ListUsageMeters(r.Context(), db.ListUsageMetersParams{
		CustomerID: ingestion.ToUUID(custID),
		Period:     pgtype.Date{Time: periodDate, Valid: true},
	})

	var usageDTOs []UsageMeterDTO
	for _, m := range meters {
		limitVal := int64(eff.Limits.MaxEventsPerMonth)
		if m.Meter != "events" {
			limitVal = 1_000_000
		}
		remaining := limitVal - m.Value
		if remaining < 0 {
			remaining = 0
		}
		usageDTOs = append(usageDTOs, UsageMeterDTO{
			Meter:     m.Meter,
			Period:    m.Period.Time.Format("2006-01-02"),
			Value:     m.Value,
			Limit:     limitVal,
			Remaining: remaining,
		})
	}

	var periodEndStr string
	if sub.PeriodEnd.Valid {
		periodEndStr = sub.PeriodEnd.Time.Format("2006-01-02")
	}

	httpresponse.JSON(w, http.StatusOK, SubscriptionDTO{
		CustomerID:  sub.CustomerID.String(),
		PlanCode:    sub.PlanCode,
		Status:      sub.Status,
		PeriodStart: sub.PeriodStart.Time.Format("2006-01-02"),
		PeriodEnd:   periodEndStr,
		Limits:      lic.LimitsDTO(limits),
		Features:    lic.FeatureSetDTO(features),
		Effective:   lic.LimitsDTO(eff.Limits),
		Usage:       usageDTOs,
	})
}

func (h *LicensingHTTPHandlers) getCustomerUsage(w http.ResponseWriter, r *http.Request) {
	custID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	periodDate := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	q := db.New(h.Pool)
	meters, err := q.ListUsageMeters(r.Context(), db.ListUsageMetersParams{
		CustomerID: ingestion.ToUUID(custID),
		Period:     pgtype.Date{Time: periodDate, Valid: true},
	})
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	var usageDTOs []UsageMeterDTO
	for _, m := range meters {
		usageDTOs = append(usageDTOs, UsageMeterDTO{
			Meter:  m.Meter,
			Period: m.Period.Time.Format("2006-01-02"),
			Value:  m.Value,
		})
	}
	httpresponse.JSON(w, http.StatusOK, usageDTOs)
}

func (h *LicensingHTTPHandlers) getCustomerUsageDaily(w http.ResponseWriter, r *http.Request) {
	custID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	startStr := r.URL.Query().Get("start_date")
	endStr := r.URL.Query().Get("end_date")
	var start, end time.Time
	if startStr != "" {
		start, _ = time.Parse("2006-01-02", startStr)
	} else {
		start = time.Now().UTC().AddDate(0, 0, -30)
	}
	if endStr != "" {
		end, _ = time.Parse("2006-01-02", endStr)
	} else {
		end = time.Now().UTC()
	}
	q := db.New(h.Pool)
	rows, err := q.ListUsageDaily(r.Context(), db.ListUsageDailyParams{
		CustomerID:  ingestion.ToUUID(custID),
		UsageDate:   pgtype.Date{Time: start, Valid: true},
		UsageDate_2: pgtype.Date{Time: end, Valid: true},
	})
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	var dtos []UsageDailyDTO
	for _, row := range rows {
		dtos = append(dtos, UsageDailyDTO{
			CustomerID: row.CustomerID.String(),
			UsageDate:  row.UsageDate.Time.Format("2006-01-02"),
			Meter:      row.Meter,
			Value:      row.Value,
		})
	}
	httpresponse.JSON(w, http.StatusOK, dtos)
}

func (h *LicensingHTTPHandlers) getCustomerQuotaStatus(w http.ResponseWriter, r *http.Request) {
	custID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	q := db.New(h.Pool)
	sub, err := q.GetCustomerSubscription(r.Context(), ingestion.ToUUID(custID))
	if err != nil {
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	var limits lic.Limits
	_ = json.Unmarshal(sub.LimitsJson, &limits)
	if len(sub.OverridesJson) > 0 {
		var overrides struct {
			Limits *lic.Limits `json:"limits,omitempty"`
		}
		if json.Unmarshal(sub.OverridesJson, &overrides) == nil && overrides.Limits != nil {
			lic.MergeLimits(&limits, *overrides.Limits)
		}
	}
	timezone := limits.QuotaResetTimezone
	if timezone == "" {
		timezone = "UTC"
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	dateStr := time.Now().In(loc).Format("20060102")
	redisKey := fmt.Sprintf("ingress:day:%s:%s", custID.String(), dateStr)
	var val int64
	if h.RedisForCustomer != nil {
		if rdb := h.RedisForCustomer(custID); rdb != nil {
			val, _ = rdb.Get(r.Context(), redisKey).Int64()
		}
	}
	remaining := int64(limits.MaxRequestsPerDay) - val
	if remaining < 0 {
		remaining = 0
	}
	httpresponse.JSON(w, http.StatusOK, QuotaStatusDTO{
		CustomerID: custID.String(),
		Limit:      int64(limits.MaxRequestsPerDay),
		Value:      val,
		Remaining:  remaining,
		Timezone:   timezone,
	})
}

func (h *LicensingHTTPHandlers) getSelfServeUsage(w http.ResponseWriter, r *http.Request) {
	custID, err := h.ResolveSelfServeCustomerID(r)
	if err != nil {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "access denied or not tenant-scoped")
		return
	}
	periodDate := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	q := db.New(h.Pool)

	sub, err := q.GetCustomerSubscription(r.Context(), ingestion.ToUUID(custID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Pro or Enterprise plan required")
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if sub.PlanCode != "pro" && sub.PlanCode != "enterprise" {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Pro or Enterprise plan required")
		return
	}

	meters, err := q.ListUsageMeters(r.Context(), db.ListUsageMetersParams{
		CustomerID: ingestion.ToUUID(custID),
		Period:     pgtype.Date{Time: periodDate, Valid: true},
	})
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	var usageDTOs []UsageMeterDTO
	for _, m := range meters {
		usageDTOs = append(usageDTOs, UsageMeterDTO{
			Meter:  m.Meter,
			Period: m.Period.Time.Format("2006-01-02"),
			Value:  m.Value,
		})
	}
	httpresponse.JSON(w, http.StatusOK, usageDTOs)
}

func (h *LicensingHTTPHandlers) getLicenseStatus(w http.ResponseWriter, r *http.Request) {
	q := db.New(h.Pool)
	licRow, err := q.GetLicenseStatus(r.Context())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "license not configured")
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	var entitlements lic.Entitlements
	_ = json.Unmarshal(licRow.EntitlementsJson, &entitlements)
	refreshMode := os.Getenv("ESPX_LICENSE_MODE")
	if refreshMode == "" {
		refreshMode = "file"
	}
	var graceEndsStr string
	if licRow.ValidUntil.Valid {
		graceEndsStr = licRow.ValidUntil.Time.Add(7 * 24 * time.Hour).Format(time.RFC3339)
	}
	httpresponse.JSON(w, http.StatusOK, lic.LicenseStatusDTO{
		DeploymentID:   licRow.DeploymentID.String(),
		LicenseID:      licRow.LicenseID.String(),
		Plan:           licRow.PlanCode,
		State:          licRow.State,
		ValidUntil:     licRow.ValidUntil.Time.Format(time.RFC3339),
		GraceEndsAt:    graceEndsStr,
		Limits:         lic.LimitsDTO(entitlements.Limits),
		Features:       lic.FeatureSetDTO(entitlements.Features),
		LastVerifiedAt: licRow.LastVerifiedAt.Time.Format(time.RFC3339),
		RefreshMode:    refreshMode,
		LastRefreshErr: licRow.LastRefreshError.String,
	})
}

func (h *LicensingHTTPHandlers) postCustomerSubscription(w http.ResponseWriter, r *http.Request) {
	custID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	var req UpdateSubscriptionRequest
	if err := lic.DecodeJSONStrict(r.Body, lic.DefaultMaxJSONBytes, &req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to decode json")
		return
	}
	pStart, err := time.Parse("2006-01-02", req.PeriodStart)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid period_start format")
		return
	}
	var pEnd pgtype.Date
	if req.PeriodEnd != "" {
		if t, parseErr := time.Parse("2006-01-02", req.PeriodEnd); parseErr == nil {
			pEnd = pgtype.Date{Time: t, Valid: true}
		}
	}
	err = pgx.BeginFunc(r.Context(), h.Pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		plan, err := q.GetSubscriptionPlan(r.Context(), req.PlanCode)
		if err != nil {
			return fmt.Errorf("plan not found: %w", err)
		}
		if licRow, err := q.GetLicenseStatus(r.Context()); err == nil {
			var licEnt lic.Entitlements
			_ = json.Unmarshal(licRow.EntitlementsJson, &licEnt)
			var limits lic.Limits
			_ = json.Unmarshal(plan.LimitsJson, &limits)
			if len(req.OverridesJSON) > 0 {
				var overrides struct {
					Limits *lic.Limits `json:"limits,omitempty"`
				}
				if json.Unmarshal(req.OverridesJSON, &overrides) == nil && overrides.Limits != nil {
					lic.MergeLimits(&limits, *overrides.Limits)
				}
			}
			if (licEnt.Limits.MaxRPS != 0 && limits.MaxRPS > licEnt.Limits.MaxRPS) ||
				(licEnt.Limits.MaxRequestsPerDay != 0 && limits.MaxRequestsPerDay > licEnt.Limits.MaxRequestsPerDay) ||
				(licEnt.Limits.MaxActiveCampaigns != 0 && limits.MaxActiveCampaigns > licEnt.Limits.MaxActiveCampaigns) {
				return errors.New("license_limit_exceeded")
			}
		}
		var overridesRaw []byte
		if len(req.OverridesJSON) > 0 {
			overridesRaw = bytes.Clone(req.OverridesJSON)
		}
		if _, err := q.UpsertCustomerSubscription(r.Context(), db.UpsertCustomerSubscriptionParams{
			CustomerID:    ingestion.ToUUID(custID),
			PlanCode:      req.PlanCode,
			Status:        req.Status,
			PeriodStart:   pgtype.Date{Time: pStart, Valid: true},
			PeriodEnd:     pEnd,
			OverridesJson: overridesRaw,
		}); err != nil {
			return err
		}
		payloadBytes, _ := json.Marshal(map[string]string{"customer_id": custID.String()})
		_, err = ingestdb.New(tx).CreateOutboxEvent(r.Context(), ingestdb.CreateOutboxEventParams{
			EventType: "UPDATE_ENTITLEMENTS",
			Payload:   payloadBytes,
		})
		return err
	})
	if err != nil {
		if err.Error() == "license_limit_exceeded" {
			httpresponse.Error(w, http.StatusForbidden, "LICENSE_LIMIT_EXCEEDED", "requested subscription limits exceed on-prem license ceiling")
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *LicensingHTTPHandlers) postCustomerQuotaBump(w http.ResponseWriter, r *http.Request) {
	custID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	var req QuotaBumpRequest
	if err := lic.DecodeJSONStrict(r.Body, lic.DefaultMaxJSONBytes, &req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to decode json")
		return
	}
	q := db.New(h.Pool)
	sub, err := q.GetCustomerSubscription(r.Context(), ingestion.ToUUID(custID))
	if err != nil {
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	var overrides struct {
		Limits   *lic.Limits     `json:"limits,omitempty"`
		Features *lic.FeatureSet `json:"features,omitempty"`
	}
	if len(sub.OverridesJson) > 0 {
		_ = json.Unmarshal(sub.OverridesJson, &overrides)
	}
	if overrides.Limits == nil {
		overrides.Limits = &lic.Limits{}
	}
	var planLimits lic.Limits
	_ = json.Unmarshal(sub.LimitsJson, &planLimits)
	if overrides.Limits.MaxRequestsPerDay == 0 {
		overrides.Limits.MaxRequestsPerDay = planLimits.MaxRequestsPerDay
	}
	overrides.Limits.MaxRequestsPerDay += uint64(req.BonusRequests)
	overridesBytes, _ := json.Marshal(overrides)
	err = pgx.BeginFunc(r.Context(), h.Pool, func(tx pgx.Tx) error {
		txq := db.New(tx)
		if _, txerr := txq.UpsertCustomerSubscription(r.Context(), db.UpsertCustomerSubscriptionParams{
			CustomerID:    ingestion.ToUUID(custID),
			PlanCode:      sub.PlanCode,
			Status:        sub.Status,
			PeriodStart:   sub.PeriodStart,
			PeriodEnd:     sub.PeriodEnd,
			OverridesJson: overridesBytes,
		}); txerr != nil {
			return txerr
		}
		payloadBytes, _ := json.Marshal(map[string]string{"customer_id": custID.String()})
		_, txerr := ingestdb.New(tx).CreateOutboxEvent(r.Context(), ingestdb.CreateOutboxEventParams{
			EventType: "UPDATE_ENTITLEMENTS",
			Payload:   payloadBytes,
		})
		return txerr
	})
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "bumped"})
}

func defaultLicenseEntitlements() lic.Entitlements {
	return lic.Entitlements{
		Limits: lic.Limits{
			MaxRPS:             999999,
			MaxRequestsPerDay:  999999999,
			MaxActiveCampaigns: 99999,
			MaxRegions:         9,
		},
		Features: lic.FeatureSet{
			RtbLive:       true,
			MlFraudBoost:  true,
			MultiRegion:   true,
			SlotMigration: true,
		},
	}
}

func applySubscriptionOverrides(raw []byte, limits *lic.Limits, features *lic.FeatureSet) {
	if len(raw) == 0 {
		return
	}
	var overrides struct {
		Limits   *lic.Limits     `json:"limits,omitempty"`
		Features *lic.FeatureSet `json:"features,omitempty"`
	}
	if json.Unmarshal(raw, &overrides) != nil {
		return
	}
	if overrides.Limits != nil {
		lic.MergeLimits(limits, *overrides.Limits)
	}
	if overrides.Features != nil {
		lic.MergeFeatures(features, *overrides.Features)
	}
}
