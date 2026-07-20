package adminapi

import (
	"errors"
	"net/http"

	"espx/internal/billing/db"
	"espx/pkg/coldpath"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ViewsHTTPHandlers serves saved report view CRUD (M6 ADM-W5).
type ViewsHTTPHandlers struct {
	Service           *Service
	Pool              *pgxpool.Pool
	ApplyRateLimit    func(http.HandlerFunc) http.HandlerFunc
	RequirePermission func(string, http.HandlerFunc) http.HandlerFunc
}

// Register mounts saved view routes on mux.
func (h *ViewsHTTPHandlers) Register(mux *http.ServeMux) {
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

	mux.HandleFunc("GET /api/v1/views", limit(perm("campaigns:read", h.listViews)))
	mux.HandleFunc("POST /api/v1/views", limit(perm("campaigns:write", h.createView)))
	mux.HandleFunc("GET /api/v1/views/{id}", limit(perm("campaigns:read", h.getView)))
	mux.HandleFunc("PUT /api/v1/views/{id}", limit(perm("campaigns:write", h.updateView)))
	mux.HandleFunc("DELETE /api/v1/views/{id}", limit(perm("campaigns:write", h.deleteView)))
}

func (h *ViewsHTTPHandlers) checkTierGate(r *http.Request, customerID uuid.UUID) (bool, error) {
	if h.Pool == nil {
		return true, nil
	}
	q := db.New(h.Pool)
	sub, err := q.GetCustomerSubscription(r.Context(), pgtype.UUID{Bytes: customerID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return sub.PlanCode == "enterprise", nil
}

func (h *ViewsHTTPHandlers) createView(w http.ResponseWriter, r *http.Request) {
	req, err := coldpath.DecodeRequest[CreateViewRequest](w, r, coldpath.DefaultMaxBody)
	if err != nil {
		return
	}

	customerID, err := uuid.Parse(req.CustomerID)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
		return
	}

	allowed, err := h.checkTierGate(r, customerID)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if !allowed {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Enterprise plan required")
		return
	}

	if h.Service == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "UNAVAILABLE", "views service not configured")
		return
	}

	ownerID := "system" // default placeholder owner
	view := h.Service.CreateView(req, ownerID)
	httpresponse.JSON(w, http.StatusCreated, view)
}

func (h *ViewsHTTPHandlers) listViews(w http.ResponseWriter, r *http.Request) {
	custIDStr := r.URL.Query().Get("customer_id")
	if custIDStr == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id query parameter is required")
		return
	}

	customerID, err := uuid.Parse(custIDStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
		return
	}

	allowed, err := h.checkTierGate(r, customerID)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if !allowed {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Enterprise plan required")
		return
	}

	if h.Service == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "UNAVAILABLE", "views service not configured")
		return
	}

	views := h.Service.ListView(custIDStr)
	httpresponse.JSON(w, http.StatusOK, views)
}

func (h *ViewsHTTPHandlers) getView(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "missing view id")
		return
	}

	if h.Service == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "UNAVAILABLE", "views service not configured")
		return
	}

	view, err := h.Service.GetView(id)
	if err != nil {
		if errors.Is(err, ErrViewNotFound) {
			httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "view not found")
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	customerID, _ := uuid.Parse(view.CustomerID)
	allowed, err := h.checkTierGate(r, customerID)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if !allowed {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Enterprise plan required")
		return
	}

	httpresponse.JSON(w, http.StatusOK, view)
}

func (h *ViewsHTTPHandlers) updateView(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "missing view id")
		return
	}

	req, err := coldpath.DecodeRequest[UpdateViewRequest](w, r, coldpath.DefaultMaxBody)
	if err != nil {
		return
	}

	if h.Service == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "UNAVAILABLE", "views service not configured")
		return
	}

	existing, err := h.Service.GetView(id)
	if err != nil {
		if errors.Is(err, ErrViewNotFound) {
			httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "view not found")
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	customerID, _ := uuid.Parse(existing.CustomerID)
	allowed, err := h.checkTierGate(r, customerID)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if !allowed {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Enterprise plan required")
		return
	}

	updated, err := h.Service.UpdateView(id, req)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	httpresponse.JSON(w, http.StatusOK, updated)
}

func (h *ViewsHTTPHandlers) deleteView(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "missing view id")
		return
	}

	if h.Service == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "UNAVAILABLE", "views service not configured")
		return
	}

	existing, err := h.Service.GetView(id)
	if err != nil {
		if errors.Is(err, ErrViewNotFound) {
			httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "view not found")
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	customerID, _ := uuid.Parse(existing.CustomerID)
	allowed, err := h.checkTierGate(r, customerID)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if !allowed {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Enterprise plan required")
		return
	}

	err = h.Service.DeleteView(id)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
