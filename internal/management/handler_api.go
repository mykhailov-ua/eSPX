package management

import (
	"net/http"
	"strconv"
	"time"

	"espx/pkg/cold"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

const maxStatsRange = 90 * 24 * time.Hour

// registerAPIRoutes mounts read-only /api/v1 reporting endpoints.
func (h *Handler) registerAPIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/campaigns/{id}/stats", h.limit(h.perm(h.getCampaignStats, PermCampaignsRead)))
	mux.HandleFunc("GET /api/v1/customers/{id}/balance", h.limit(h.perm(h.getCustomerBalance, PermCustomersRead)))
	mux.HandleFunc("GET /api/v1/customers/{id}/balance/export", h.limit(h.limitExportByCustomer(h.perm(h.exportCustomerBalance, PermCustomersRead))))
	mux.HandleFunc("GET /api/v1/recon/runs", h.limit(h.perm(h.listReconRuns, PermAuditRead)))
	mux.HandleFunc("GET /api/v1/disputes", h.limit(h.perm(h.listDisputes, PermCustomersRead)))
	mux.HandleFunc("POST /api/v1/forecast/campaign", h.limit(h.perm(h.forecastCampaign, PermCampaignsRead)))
	mux.HandleFunc("POST /api/v1/consent", h.limit(h.postConsent))
}

// getCampaignStats handles GET /api/v1/campaigns/{id}/stats.
func (h *Handler) getCampaignStats(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}

	if err := h.ensureCampaignAccess(r, campaignID); err != nil {
		writeServiceError(w, err)
		return
	}

	from, to, granularity, err := parseStatsQuery(r)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	report, err := h.svc.GetCampaignStats(r.Context(), campaignID, from, to, granularity)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	httpresponse.JSON(w, http.StatusOK, report)
}

func parseStatsQuery(r *http.Request) (from, to time.Time, granularity string, err error) {
	granularity = r.URL.Query().Get("granularity")
	if granularity == "" {
		granularity = "hour"
	}
	if granularity != "hour" {
		return time.Time{}, time.Time{}, "", errInvalidQuery("granularity must be hour")
	}

	now := time.Now().UTC().Truncate(time.Hour)
	to = now
	from = now.Add(-7 * 24 * time.Hour)

	if toStr := r.URL.Query().Get("to"); toStr != "" {
		to, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			return time.Time{}, time.Time{}, "", errInvalidQuery("invalid to timestamp")
		}
		to = to.UTC()
	}
	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		from, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return time.Time{}, time.Time{}, "", errInvalidQuery("invalid from timestamp")
		}
		from = from.UTC()
	}

	if !to.After(from) {
		return time.Time{}, time.Time{}, "", errInvalidQuery("to must be after from")
	}
	if to.Sub(from) > maxStatsRange {
		return time.Time{}, time.Time{}, "", errInvalidQuery("time range exceeds 90 days")
	}
	return from, to, granularity, nil
}

type invalidQueryError string

func errInvalidQuery(msg string) error {
	return invalidQueryError(msg)
}

func (e invalidQueryError) Error() string { return string(e) }

// parseAPIPagination applies M1 defaults: limit default 50, max 1000.
func parseAPIPagination(r *http.Request) (int32, int32) {
	limit := int32(50)
	if l, err := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32); err == nil && l > 0 {
		limit = int32(l)
	}
	offset := int32(0)
	if o, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 32); err == nil && o > 0 {
		offset = int32(o)
	}
	return cold.ClampLimitOffset(limit, offset, 50, 1000)
}
