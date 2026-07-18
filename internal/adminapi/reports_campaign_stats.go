package adminapi

import (
	"context"
	"net/http"
	"time"

	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

// CampaignStatsReader loads merged Postgres + ClickHouse campaign stats.
type CampaignStatsReader interface {
	GetCampaignStats(ctx context.Context, campaignID uuid.UUID, from, to time.Time, granularity string) (CampaignStatsDTO, error)
}

func (h *ReportsHTTPHandlers) registerCampaignStats(mux *http.ServeMux) {
	if h.CampaignStats == nil {
		return
	}
	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	mux.HandleFunc("GET /api/v1/campaigns/{id}/stats", limit(perm("campaigns:read", h.getCampaignStats)))
}

func (h *ReportsHTTPHandlers) getCampaignStats(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}

	if h.AuthorizeCampaignAccess != nil {
		if err := h.AuthorizeCampaignAccess(r, campaignID); err != nil {
			h.writeServiceError(w, err)
			return
		}
	}

	from, to, granularity, err := parseStatsQuery(r)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	report, err := h.CampaignStats.GetCampaignStats(r.Context(), campaignID, from, to, granularity)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	httpresponse.JSON(w, http.StatusOK, report)
}
