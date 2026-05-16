package management

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/shopspring/decimal"
	"golang.org/x/time/rate"
)

type Handler struct {
	svc            *Service
	cfg            *config.Config
	limiter        *rate.Limiter
	authMiddleware *AuthMiddleware
}

func NewHandler(svc *Service, cfg *config.Config, authMiddleware *AuthMiddleware) *Handler {
	return &Handler{
		svc:            svc,
		cfg:            cfg,
		limiter:        rate.NewLimiter(rate.Limit(10), 50), // 10 req/s, burst 50
		authMiddleware: authMiddleware,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/customers", h.limit(h.auth(h.createCustomer, "SA", "M")))
	mux.HandleFunc("POST /admin/customers/{id}/topup", h.limit(h.auth(h.topUpBalance, "SA", "M")))
	mux.HandleFunc("POST /admin/campaigns", h.limit(h.auth(h.createCampaign, "SA", "M", "C")))
	mux.HandleFunc("DELETE /admin/campaigns/{id}", h.limit(h.auth(h.cancelCampaign, "SA", "M", "C")))

	// New routes
	mux.HandleFunc("POST /admin/settings", h.limit(h.auth(h.updateSettings, "SA")))
	mux.HandleFunc("POST /admin/blacklist", h.limit(h.auth(h.blockIP, "SA")))
	mux.HandleFunc("DELETE /admin/blacklist", h.limit(h.auth(h.unblockIP, "SA")))
	mux.HandleFunc("GET /admin/audit", h.limit(h.auth(h.listAudit, "SA", "M")))

	// Customer GET routes
	mux.HandleFunc("GET /admin/customers", h.limit(h.auth(h.listCustomers, "SA", "M")))
	mux.HandleFunc("GET /admin/customers/{id}", h.limit(h.auth(h.getCustomer, "SA", "M", "C")))
	mux.HandleFunc("GET /admin/customers/{id}/ledger", h.limit(h.auth(h.getCustomerLedger, "SA", "M", "C")))

	// Campaign GET routes
	mux.HandleFunc("GET /admin/campaigns", h.limit(h.auth(h.listCampaigns, "SA", "M", "C")))
	mux.HandleFunc("GET /admin/campaigns/{id}", h.limit(h.auth(h.getCampaign, "SA", "M", "C")))
	mux.HandleFunc("GET /admin/campaigns/{id}/history", h.limit(h.auth(h.getCampaignHistory, "SA", "M", "C")))

	// System GET routes
	mux.HandleFunc("GET /admin/blacklist", h.limit(h.auth(h.listBlacklist, "SA")))
	mux.HandleFunc("GET /admin/settings", h.limit(h.auth(h.getSettings, "SA")))
}

func (h *Handler) limit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.limiter.Allow() {
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

func (h *Handler) auth(next http.HandlerFunc, allowedRoles ...string) http.HandlerFunc {
	if h.authMiddleware != nil {
		return h.authMiddleware.RequireAuth(allowedRoles...)(next)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Admin-API-Key")
		if key == "" || key != string(h.cfg.AdminAPIKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (h *Handler) createCustomer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       uuid.UUID       `json:"id"`
		Name     string          `json:"name"`
		Balance  decimal.Decimal `json:"balance"`
		Currency string          `json:"currency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.ID == uuid.Nil {
		req.ID, _ = uuid.NewV7()
	}
	if err := h.svc.CreateCustomer(r.Context(), req.ID, req.Name, req.Balance, req.Currency); err != nil {
		slog.Error("failed to create customer", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": req.ID})
}

func (h *Handler) topUpBalance(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid customer id", http.StatusBadRequest)
		return
	}
	var req struct {
		Amount decimal.Decimal `json:"amount"`
	}
	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	hash := h.svc.GenerateIdempotencyHash(customerID, req)
	if err := h.svc.TopUpBalance(r.Context(), customerID, req.Amount, hash); err != nil {
		slog.Error("failed to top up balance", "error", err, "customer_id", customerID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) createCampaign(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID      uuid.UUID       `json:"customer_id"`
		Name            string          `json:"name"`
		BudgetLimit     decimal.Decimal `json:"budget_limit"`
		PacingMode      string          `json:"pacing_mode"`
		DailyBudget     decimal.Decimal `json:"daily_budget"`
		Timezone        string          `json:"timezone"`
		FreqLimit       int32           `json:"freq_limit"`
		FreqWindow      int32           `json:"freq_window"`
		TargetCountries []string        `json:"target_countries"`
	}
	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.CustomerID == uuid.Nil {
		http.Error(w, "customer_id is required", http.StatusBadRequest)
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" && req.CustomerID != u.CustomerID {
		http.Error(w, "forbidden: cannot create campaign for another customer", http.StatusForbidden)
		return
	}

	// Defaults
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

	hash := h.svc.GenerateIdempotencyHash(req.CustomerID, req)
	id, err := h.svc.CreateCampaign(r.Context(), req.CustomerID, req.Name, req.BudgetLimit, pacing, req.DailyBudget, req.Timezone, req.FreqLimit, req.FreqWindow, req.TargetCountries, hash)
	if err != nil {
		slog.Error("failed to create campaign", "error", err, "customer_id", req.CustomerID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
}

func (h *Handler) cancelCampaign(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid campaign id", http.StatusBadRequest)
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			http.Error(w, "forbidden: campaign belongs to another customer", http.StatusForbidden)
			return
		}
	}

	if err := h.svc.CancelCampaign(r.Context(), campaignID, req.Reason); err != nil {
		slog.Error("failed to cancel campaign", "error", err, "campaign_id", campaignID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) updateSettings(w http.ResponseWriter, r *http.Request) {
	var settings map[string]string
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.svc.UpdateSettings(r.Context(), settings); err != nil {
		slog.Error("failed to update settings", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) blockIP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IP     string `json:"ip"`
		Source string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IP == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.svc.BlockIP(r.Context(), req.IP, req.Source); err != nil {
		slog.Error("failed to block ip", "error", err, "ip", req.IP)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) unblockIP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IP     string `json:"ip"`
		Source string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IP == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.svc.UnblockIP(r.Context(), req.IP, req.Source); err != nil {
		slog.Error("failed to unblock ip", "error", err, "ip", req.IP)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listAudit(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := int32(50)
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
		limit = int32(l)
	}

	offset := int32(0)
	if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
		offset = int32(o)
	}

	logs, err := h.svc.ListAuditLogs(r.Context(), limit, offset)
	if err != nil {
		slog.Error("failed to list audit logs", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(logs)
}

func parsePagination(r *http.Request) (int32, int32) {
	limit := int32(20)
	if l, err := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32); err == nil && l > 0 {
		limit = int32(l)
		if limit > 100 {
			limit = 100
		}
	}
	offset := int32(0)
	if o, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 32); err == nil && o > 0 {
		offset = int32(o)
	}
	return limit, offset
}

func (h *Handler) listCustomers(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	customers, total, err := h.svc.ListCustomers(r.Context(), limit, offset)
	if err != nil {
		slog.Error("failed to list customers", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(customers)
}

func (h *Handler) getCustomer(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid customer id", http.StatusBadRequest)
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" && u.CustomerID != customerID {
		http.Error(w, "forbidden: cannot access another customer", http.StatusForbidden)
		return
	}

	customer, err := h.svc.GetCustomerDTO(r.Context(), customerID)
	if err != nil {
		http.Error(w, "customer not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(customer)
}

func (h *Handler) getCustomerLedger(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid customer id", http.StatusBadRequest)
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" && u.CustomerID != customerID {
		http.Error(w, "forbidden: cannot access another customer", http.StatusForbidden)
		return
	}

	limit, offset := parsePagination(r)
	ledger, total, err := h.svc.ListCustomerLedger(r.Context(), customerID, limit, offset)
	if err != nil {
		slog.Error("failed to list customer ledger", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ledger)
}

func (h *Handler) listCampaigns(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	status := r.URL.Query().Get("status")

	var custID uuid.UUID
	if cStr := r.URL.Query().Get("customer_id"); cStr != "" {
		if id, err := uuid.Parse(cStr); err == nil {
			custID = id
		}
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" {
		custID = u.CustomerID
	}

	campaigns, total, err := h.svc.ListCampaigns(r.Context(), custID, status, limit, offset)
	if err != nil {
		slog.Error("failed to list campaigns", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(campaigns)
}

func (h *Handler) getCampaign(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid campaign id", http.StatusBadRequest)
		return
	}

	campaign, err := h.svc.GetCampaignDTO(r.Context(), campaignID)
	if err != nil {
		http.Error(w, "campaign not found", http.StatusNotFound)
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" && campaign.CustomerID != u.CustomerID.String() {
		http.Error(w, "forbidden: cannot access another customer's campaign", http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(campaign)
}

func (h *Handler) getCampaignHistory(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid campaign id", http.StatusBadRequest)
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			http.Error(w, "forbidden: cannot access another customer's campaign", http.StatusForbidden)
			return
		}
	}

	limit, offset := parsePagination(r)
	history, total, err := h.svc.ListStatusHistory(r.Context(), campaignID, limit, offset)
	if err != nil {
		slog.Error("failed to list campaign history", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(history)
}

func (h *Handler) listBlacklist(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, total, err := h.svc.ListBlacklist(r.Context(), limit, offset)
	if err != nil {
		slog.Error("failed to list blacklist", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(items)
}

func (h *Handler) getSettings(w http.ResponseWriter, r *http.Request) {
	settings, err := h.svc.GetSettings(r.Context())
	if err != nil {
		slog.Error("failed to get settings", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(settings)
}
