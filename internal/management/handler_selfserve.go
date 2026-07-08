package management

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"espx/internal/ads/db"
	"espx/pkg/cold"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// registerSelfServeRoutes mounts customer-facing /api/v1/selfserve endpoints (M4.1–M4.5).
func (h *Handler) registerSelfServeRoutes(mux *http.ServeMux) {
	ss := h.selfServePerm
	mux.HandleFunc("POST /api/v1/selfserve/campaigns", h.limit(ss(h.createSelfServeCampaign, PermCampaignsWrite)))
	mux.HandleFunc("POST /api/v1/selfserve/campaigns/{id}/pause", h.limit(ss(h.pauseSelfServeCampaign, PermCampaignsWrite)))
	mux.HandleFunc("POST /api/v1/selfserve/campaigns/{id}/resume", h.limit(ss(h.resumeSelfServeCampaign, PermCampaignsWrite)))
	mux.HandleFunc("POST /api/v1/selfserve/payment-intents", h.limit(ss(h.createSelfServePaymentIntent, PermCustomersRead)))
	mux.HandleFunc("GET /api/v1/selfserve/invoices", h.limit(ss(h.listSelfServeInvoices, PermCustomersRead)))
	mux.HandleFunc("POST /api/v1/selfserve/api-keys", h.limit(ss(h.createSelfServeAPIKey, PermCampaignsWrite)))
}

func (h *Handler) selfServePerm(next http.HandlerFunc, permission string) http.HandlerFunc {
	if h.authMiddleware != nil {
		return h.authMiddleware.RequireSelfServe(permission)(next)
	}
	return h.perm(next, permission)
}

// createSelfServeCampaign handles POST /api/v1/selfserve/campaigns for tenant-scoped campaign creation.
func (h *Handler) createSelfServeCampaign(w http.ResponseWriter, r *http.Request) {
	body, err := cold.ReadLimitedBody(w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to read request body")
		return
	}

	req, err := cold.DecodeBody[struct {
		CustomerID       *uuid.UUID `json:"customer_id,omitempty"`
		BrandID          *uuid.UUID `json:"brand_id,omitempty"`
		Name             string     `json:"name"`
		BudgetLimitMicro *int64     `json:"budget_limit_micro"`
		BudgetLimit      *float64   `json:"budget_limit"`
		PacingMode       string     `json:"pacing_mode"`
		DailyBudgetMicro *int64     `json:"daily_budget_micro"`
		DailyBudget      *float64   `json:"daily_budget"`
		Timezone         string     `json:"timezone"`
		FreqLimit        int32      `json:"freq_limit"`
		FreqWindow       int32      `json:"freq_window"`
		TargetCountries  []string   `json:"target_countries"`
		StartAt          *time.Time `json:"start_at,omitempty"`
		EndAt            *time.Time `json:"end_at,omitempty"`
		DaypartHours     []int16    `json:"daypart_hours,omitempty"`
	}](body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if req.Name == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "name is required")
		return
	}

	customerID, err := h.resolveSelfServeCustomerID(r, req.CustomerID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "Idempotency-Key header is required")
		return
	}

	pacing := db.PacingModeTypeASAP
	if req.PacingMode == "EVEN" {
		pacing = db.PacingModeTypeEVEN
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if req.FreqWindow == 0 {
		req.FreqWindow = 86400
	}

	budgetLegacy := 0.0
	hasBudgetLegacy := req.BudgetLimit != nil
	if hasBudgetLegacy {
		budgetLegacy = *req.BudgetLimit
	}
	budgetLimitMicro, err := parseBudgetMicro(req.BudgetLimitMicro, budgetLegacy, hasBudgetLegacy)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	dailyLegacy := 0.0
	hasDailyLegacy := req.DailyBudget != nil
	if hasDailyLegacy {
		dailyLegacy = *req.DailyBudget
	}
	dailyBudgetMicro, err := parseMoneyMicro(req.DailyBudgetMicro, dailyLegacy, hasDailyLegacy, "daily_budget")
	if err != nil {
		writeServiceError(w, err)
		return
	}

	if err := h.svc.EnforceSelfServeCreateLimits(r.Context(), customerID, budgetLimitMicro); err != nil {
		writeServiceError(w, err)
		return
	}

	hash, err := h.svc.GenerateIdempotencyHash(customerID, append(body, []byte(idempotencyKey)...))
	if err != nil {
		writeServiceError(w, err, slog.String("customer_id", customerID.String()))
		return
	}

	id, err := h.svc.CreateCampaign(r.Context(), CampaignCreateSpec{
		CustomerID:      customerID,
		BrandID:         req.BrandID,
		Name:            req.Name,
		BudgetLimit:     budgetLimitMicro,
		PacingMode:      pacing,
		DailyBudget:     dailyBudgetMicro,
		Timezone:        req.Timezone,
		FreqLimit:       req.FreqLimit,
		FreqWindow:      req.FreqWindow,
		TargetCountries: req.TargetCountries,
		StartAt:         req.StartAt,
		EndAt:           req.EndAt,
		DaypartHours:    req.DaypartHours,
		IdempotencyKey:  hash,
	})
	if err != nil {
		writeServiceError(w, err, slog.String("customer_id", customerID.String()))
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (h *Handler) pauseSelfServeCampaign(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	if err := h.ensureCampaignAccess(r, campaignID); err != nil {
		writeServiceError(w, err)
		return
	}
	req, err := cold.DecodeRequest[struct {
		Reason string `json:"reason"`
	}](w, r, cold.DefaultMaxBody)
	if err != nil {
		slog.Warn("failed to decode pause campaign request", "error", err)
	}
	if err := h.svc.PauseCampaign(r.Context(), campaignID, req.Reason); err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) resumeSelfServeCampaign(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	if err := h.ensureCampaignAccess(r, campaignID); err != nil {
		writeServiceError(w, err)
		return
	}
	req, err := cold.DecodeRequest[struct {
		Reason string `json:"reason"`
	}](w, r, cold.DefaultMaxBody)
	if err != nil {
		slog.Warn("failed to decode resume campaign request", "error", err)
	}
	if err := h.svc.ResumeCampaign(r.Context(), campaignID, req.Reason); err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

type selfServePaymentIntentRequest struct {
	CustomerID  *uuid.UUID `json:"customer_id,omitempty"`
	AmountMicro int64      `json:"amount_micro"`
	Currency    string     `json:"currency"`
}

// createSelfServePaymentIntent proxies top-ups to payment gRPC for authenticated tenants.
func (h *Handler) createSelfServePaymentIntent(w http.ResponseWriter, r *http.Request) {
	body, err := cold.ReadLimitedBody(w, r, 16*1024)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}

	req, err := cold.DecodeBody[selfServePaymentIntentRequest](body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if req.AmountMicro <= 0 {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "amount_micro must be greater than zero")
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "Idempotency-Key header is required")
		return
	}

	if h.payment == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "PAYMENT_UNAVAILABLE", "payment service not configured")
		return
	}

	customerID, err := h.resolveSelfServeCustomerID(r, req.CustomerID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	currency := req.Currency
	if currency == "" {
		currency = "USD"
	}

	meta := map[string]string{
		"customer_id": customerID.String(),
		"source":      "selfserve",
	}

	resp, err := h.payment.CreatePaymentIntent(r.Context(), customerID.String(), req.AmountMicro, currency, idempotencyKey, meta)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", st.Message())
				return
			case codes.AlreadyExists:
				httpresponse.Error(w, http.StatusConflict, "CONFLICT", st.Message())
				return
			case codes.FailedPrecondition:
				httpresponse.Error(w, http.StatusServiceUnavailable, "PAYMENT_UNAVAILABLE", st.Message())
				return
			}
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "failed to create payment intent")
		return
	}

	httpresponse.JSON(w, http.StatusOK, map[string]any{
		"intent_id":    resp.IntentId,
		"status":       resp.Status.String(),
		"checkout_url": resp.CheckoutUrl,
		"provider_ref": resp.ProviderRef,
	})
}

// listSelfServeInvoices handles GET /api/v1/selfserve/invoices for tenant billing history.
func (h *Handler) listSelfServeInvoices(w http.ResponseWriter, r *http.Request) {
	if h.billing == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing service not configured")
		return
	}

	var bodyCustomerID *uuid.UUID
	if raw := r.URL.Query().Get("customer_id"); raw != "" {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
			return
		}
		bodyCustomerID = &parsed
	}

	customerID, err := h.resolveSelfServeCustomerID(r, bodyCustomerID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	limit := int32(20)
	offset := int32(0)
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = int32(n)
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = int32(n)
		}
	}
	if limit > 100 {
		limit = 100
	}

	resp, err := h.billing.ListInvoices(r.Context(), customerID.String(), limit, offset)
	if err != nil {
		writeBillingGRPCError(w, err)
		return
	}

	invoices := make([]map[string]any, 0, len(resp.Invoices))
	for _, inv := range resp.Invoices {
		invoices = append(invoices, invoiceToJSON(inv))
	}
	httpresponse.JSON(w, http.StatusOK, map[string]any{
		"invoices": invoices,
		"total":    resp.Total,
	})
}

// createSelfServeAPIKey mints a machine credential for the authenticated session user.
func (h *Handler) createSelfServeAPIKey(w http.ResponseWriter, r *http.Request) {
	if h.authClient == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "AUTH_UNAVAILABLE", "auth service not configured")
		return
	}

	req, err := cold.DecodeRequest[struct {
		Name string `json:"name"`
	}](w, r, cold.DefaultMaxBody)
	if err != nil {
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "name is required")
		return
	}

	cookie, err := r.Cookie("accessToken")
	if err != nil || cookie.Value == "" {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "session required to create api keys")
		return
	}

	resp, err := h.authClient.CreateAPIKey(r.Context(), cookie.Value, req.Name)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", st.Message())
				return
			case codes.Unauthenticated:
				httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", st.Message())
				return
			}
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "failed to create api key")
		return
	}

	out := map[string]any{
		"id":      resp.Id,
		"name":    resp.Name,
		"raw_key": resp.RawKey,
	}
	if resp.ExpiresAt != nil {
		out["expires_at"] = resp.ExpiresAt.AsTime().UTC().Format(time.RFC3339)
	}
	httpresponse.JSON(w, http.StatusCreated, out)
}

// resolveSelfServeCustomerID binds tenant context: role U uses session customer_id; staff may pass customer_id.
func (h *Handler) resolveSelfServeCustomerID(r *http.Request, bodyCustomerID *uuid.UUID) (uuid.UUID, error) {
	u, ok := GetUser(r.Context())
	if !ok {
		return uuid.Nil, errForbidden
	}
	if u.IsUser() {
		if bodyCustomerID != nil && *bodyCustomerID != uuid.Nil && *bodyCustomerID != u.CustomerID {
			return uuid.Nil, errForbidden
		}
		return u.CustomerID, nil
	}
	if bodyCustomerID == nil || *bodyCustomerID == uuid.Nil {
		return uuid.Nil, errValidation("customer_id is required")
	}
	return *bodyCustomerID, nil
}
