package adminapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"espx/internal/costsync"
	db "espx/internal/ingestion/sqlc"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CostSyncHTTPHandlers exposes M16 admin endpoints for credentials and manual sync.
type CostSyncHTTPHandlers struct {
	Pool              *pgxpool.Pool
	EncryptionKey     []byte
	Worker            *costsync.Worker
	ApplyRateLimit    func(http.HandlerFunc) http.HandlerFunc
	RequirePermission func(string, http.HandlerFunc) http.HandlerFunc
}

func (h *CostSyncHTTPHandlers) Register(mux *http.ServeMux) {
	if h == nil {
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

	mux.HandleFunc("GET /api/v1/cost-sync/credentials", limit(perm("campaigns:read", h.listCredentials)))
	mux.HandleFunc("PUT /api/v1/cost-sync/credentials/{network}", limit(perm("campaigns:write", h.upsertCredential)))
	mux.HandleFunc("DELETE /api/v1/cost-sync/credentials/{network}", limit(perm("campaigns:write", h.deleteCredential)))
	mux.HandleFunc("POST /api/v1/cost-sync/run", limit(perm("campaigns:write", h.runSync)))
	mux.HandleFunc("GET /api/v1/cost-sync/history", limit(perm("campaigns:read", h.listHistory)))
}

type CostSyncCredentialDTO struct {
	CustomerID string            `json:"customer_id"`
	Network    string            `json:"network"`
	AccountID  string            `json:"account_id"`
	Extra      map[string]string `json:"extra_config,omitempty"`
	ExpiresAt  *time.Time        `json:"token_expires_at,omitempty"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type UpsertCostSyncCredentialRequest struct {
	CustomerID   string            `json:"customer_id"`
	AccountID    string            `json:"account_id"`
	AccessToken  string            `json:"access_token"`
	RefreshToken string            `json:"refresh_token"`
	APIKey       string            `json:"api_key"`
	ExtraConfig  map[string]string `json:"extra_config"`
}

type RunCostSyncRequest struct {
	CustomerID string `json:"customer_id"`
	Network    string `json:"network"`
	From       string `json:"from"`
	To         string `json:"to"`
}

type CostSyncRunDTO struct {
	ID                  int64      `json:"id"`
	CustomerID          string     `json:"customer_id"`
	Network             string     `json:"network"`
	CostDate            string     `json:"cost_date"`
	Status              string     `json:"status"`
	RowsImported        int32      `json:"rows_imported"`
	TotalAmountUSDMicro int64      `json:"total_amount_usd_micro"`
	ErrorMessage        string     `json:"error_message,omitempty"`
	TriggerSource       string     `json:"trigger_source"`
	StartedAt           time.Time  `json:"started_at"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
}

func (h *CostSyncHTTPHandlers) listCredentials(w http.ResponseWriter, r *http.Request) {
	q := db.New(h.Pool)
	var rows []db.CostSyncCredential
	var err error

	if custStr := r.URL.Query().Get("customer_id"); custStr != "" {
		custID, parseErr := uuid.Parse(custStr)
		if parseErr != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
			return
		}
		rows, err = q.ListCostSyncCredentialsByCustomer(r.Context(), pgtype.UUID{Bytes: custID, Valid: true})
	} else {
		rows, err = q.ListCostSyncCredentials(r.Context())
	}
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	dtos := make([]CostSyncCredentialDTO, 0, len(rows))
	for _, row := range rows {
		dto := CostSyncCredentialDTO{
			CustomerID: ingestionUUIDToString(row.CustomerID),
			Network:    row.Network,
			AccountID:  row.AccountID,
			UpdatedAt:  row.UpdatedAt.Time,
		}
		if len(row.ExtraConfig) > 0 {
			_ = json.Unmarshal(row.ExtraConfig, &dto.Extra)
		}
		if row.TokenExpiresAt.Valid {
			t := row.TokenExpiresAt.Time
			dto.ExpiresAt = &t
		}
		dtos = append(dtos, dto)
	}
	httpresponse.JSON(w, http.StatusOK, dtos)
}

func (h *CostSyncHTTPHandlers) upsertCredential(w http.ResponseWriter, r *http.Request) {
	network := r.PathValue("network")
	if network == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "missing network")
		return
	}

	var req UpsertCostSyncCredentialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json body")
		return
	}
	custID, err := uuid.Parse(req.CustomerID)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
		return
	}

	allowed, err := h.checkProTier(r, custID)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if !allowed {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Pro or Enterprise plan required")
		return
	}

	accessEnc, refreshEnc, apiEnc, err := costsync.EncryptCredentialFields(h.EncryptionKey, req.AccessToken, req.RefreshToken, req.APIKey)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	extraRaw, _ := json.Marshal(req.ExtraConfig)
	row, err := db.New(h.Pool).UpsertCostSyncCredential(r.Context(), db.UpsertCostSyncCredentialParams{
		CustomerID:            pgtype.UUID{Bytes: custID, Valid: true},
		Network:               network,
		AccountID:             req.AccountID,
		AccessTokenEncrypted:  accessEnc,
		RefreshTokenEncrypted: refreshEnc,
		ApiKeyEncrypted:       apiEnc,
		ExtraConfig:           extraRaw,
	})
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	httpresponse.JSON(w, http.StatusOK, CostSyncCredentialDTO{
		CustomerID: ingestionUUIDToString(row.CustomerID),
		Network:    row.Network,
		AccountID:  row.AccountID,
		UpdatedAt:  row.UpdatedAt.Time,
	})
}

func (h *CostSyncHTTPHandlers) deleteCredential(w http.ResponseWriter, r *http.Request) {
	network := r.PathValue("network")
	custStr := r.URL.Query().Get("customer_id")
	custID, err := uuid.Parse(custStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
		return
	}

	err = db.New(h.Pool).DeleteCostSyncCredential(r.Context(), db.DeleteCostSyncCredentialParams{
		CustomerID: pgtype.UUID{Bytes: custID, Valid: true},
		Network:    network,
	})
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *CostSyncHTTPHandlers) runSync(w http.ResponseWriter, r *http.Request) {
	if h.Worker == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "UNAVAILABLE", "cost-sync worker not configured")
		return
	}

	var req RunCostSyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json body")
		return
	}

	var custFilter *uuid.UUID
	if req.CustomerID != "" {
		cid, err := uuid.Parse(req.CustomerID)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
			return
		}
		custFilter = &cid
	}

	from := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	to := from
	if req.From != "" {
		parsed, err := time.Parse("2006-01-02", req.From)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid from date")
			return
		}
		from = parsed
	}
	if req.To != "" {
		parsed, err := time.Parse("2006-01-02", req.To)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid to date")
			return
		}
		to = parsed
	}

	if err := h.Worker.RunManual(r.Context(), custFilter, req.Network, from, to); err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (h *CostSyncHTTPHandlers) listHistory(w http.ResponseWriter, r *http.Request) {
	var cust pgtype.UUID
	if custStr := r.URL.Query().Get("customer_id"); custStr != "" {
		cid, err := uuid.Parse(custStr)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
			return
		}
		cust = pgtype.UUID{Bytes: cid, Valid: true}
	}

	limit := int32(50)
	offset := int32(0)
	rows, err := db.New(h.Pool).ListCostSyncRuns(r.Context(), db.ListCostSyncRunsParams{
		Column1: cust,
		Limit:   limit,
		Offset:  offset,
	})
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	dtos := make([]CostSyncRunDTO, 0, len(rows))
	for _, row := range rows {
		dto := CostSyncRunDTO{
			ID:                  row.ID,
			CustomerID:          ingestionUUIDToString(row.CustomerID),
			Network:             row.Network,
			CostDate:            row.CostDate.Time.Format("2006-01-02"),
			Status:              row.Status,
			RowsImported:        row.RowsImported,
			TotalAmountUSDMicro: row.TotalAmountUsdMicro,
			TriggerSource:       row.TriggerSource,
			StartedAt:           row.StartedAt.Time,
		}
		if row.ErrorMessage.Valid {
			dto.ErrorMessage = row.ErrorMessage.String
		}
		if row.CompletedAt.Valid {
			t := row.CompletedAt.Time
			dto.CompletedAt = &t
		}
		dtos = append(dtos, dto)
	}
	httpresponse.JSON(w, http.StatusOK, dtos)
}

func (h *CostSyncHTTPHandlers) checkProTier(r *http.Request, customerID uuid.UUID) (bool, error) {
	if h.Pool == nil {
		return true, nil
	}
	var planCode string
	err := h.Pool.QueryRow(r.Context(), "SELECT plan_code FROM billing.customer_subscriptions WHERE customer_id = $1", customerID).Scan(&planCode)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return strings.ToLower(planCode) == "pro" || strings.ToLower(planCode) == "enterprise", nil
}
