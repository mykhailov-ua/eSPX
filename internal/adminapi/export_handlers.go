package adminapi

import (
	"errors"
	"espx/pkg/coldpath"
	"io"
	"net/http"

	"espx/pkg/httpresponse"
)

// ExportHTTPHandlers serves EXP-02/03 billing export routes.
type ExportHTTPHandlers struct {
	JobRunner               *JobRunner
	ApplyRateLimit          func(http.HandlerFunc) http.HandlerFunc
	RequirePermission       func(string, http.HandlerFunc) http.HandlerFunc
	AuthorizeCustomerAccess func(*http.Request, string) error
	WriteServiceError       func(http.ResponseWriter, error)
}

// Register mounts billing export routes.
func (h *ExportHTTPHandlers) Register(mux *http.ServeMux) {
	if h == nil || h.JobRunner == nil {
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
	mux.HandleFunc("POST /api/v1/billing/exports", limit(perm("customers:read", h.createExport)))
	mux.HandleFunc("GET /api/v1/billing/exports/{job_id}", limit(perm("customers:read", h.getExport)))
	mux.HandleFunc("GET /api/v1/billing/exports/{job_id}/download", limit(perm("customers:read", h.downloadExport)))
}

func (h *ExportHTTPHandlers) createExport(w http.ResponseWriter, r *http.Request) {
	body, err := coldpath.ReadLimitedBody(w, r, coldpath.DefaultMaxBody)
	if err != nil {
		return
	}
	spec, err := coldpath.DecodeBody[JobSpec](body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if spec.CustomerID == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
		return
	}
	if h.AuthorizeCustomerAccess != nil {
		if err := h.AuthorizeCustomerAccess(r, spec.CustomerID); err != nil {
			h.writeServiceError(w, err)
			return
		}
	}
	jobID, err := h.JobRunner.CreateJob(r.Context(), spec)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	w.Header().Set("Location", "/api/v1/billing/exports/"+jobID)
	httpresponse.JSON(w, http.StatusAccepted, map[string]any{"job_id": jobID})
}

func (h *ExportHTTPHandlers) getExport(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	status, ok := h.JobRunner.GetJob(jobID)
	if !ok {
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "export job not found")
		return
	}
	if h.AuthorizeCustomerAccess != nil {
		if err := h.AuthorizeCustomerAccess(r, status.CustomerID); err != nil {
			h.writeServiceError(w, err)
			return
		}
	}
	httpresponse.JSON(w, http.StatusOK, status)
}

func (h *ExportHTTPHandlers) downloadExport(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	f, status, err := h.JobRunner.OpenDownload(jobID)
	if err != nil {
		if status.ID != "" {
			if h.AuthorizeCustomerAccess != nil {
				if aerr := h.AuthorizeCustomerAccess(r, status.CustomerID); aerr != nil {
					h.writeServiceError(w, aerr)
					return
				}
			}
			httpresponse.Error(w, http.StatusConflict, "NOT_READY", err.Error())
			return
		}
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "export job not found")
		return
	}
	defer f.Close()
	if h.AuthorizeCustomerAccess != nil {
		if err := h.AuthorizeCustomerAccess(r, status.CustomerID); err != nil {
			h.writeServiceError(w, err)
			return
		}
	}
	if status.Format == "ndjson" {
		w.Header().Set("Content-Type", "application/x-ndjson")
	} else {
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	}
	w.Header().Set("Content-Disposition", "attachment; filename=\"billing-export-"+jobID+"."+status.Format+"\"")
	_, _ = io.Copy(w, f)
}

func (h *ExportHTTPHandlers) writeServiceError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrForbidden) {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	if h.WriteServiceError != nil {
		h.WriteServiceError(w, err)
		return
	}
	httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "request failed")
}
