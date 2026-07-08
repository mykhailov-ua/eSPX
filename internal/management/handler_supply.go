package management

import (
	"log/slog"
	"net/http"
	"strconv"

	"espx/pkg/cold"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

// registerSupplyRoutes mounts IAB supply chain admin and public endpoints.
func (h *Handler) registerSupplyRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /.well-known/sellers.json", h.limit(h.getSellersJSON))
	mux.HandleFunc("GET /admin/supply/ads.txt", h.limit(h.perm(h.exportAdsTxt, PermSettingsRead)))
	mux.HandleFunc("GET /admin/supply/sellers", h.limit(h.perm(h.listSellers, PermSettingsRead)))
	mux.HandleFunc("POST /admin/supply/sellers", h.limit(h.perm(h.createSeller, PermSettingsWrite)))
	mux.HandleFunc("GET /admin/supply/sellers/{id}", h.limit(h.perm(h.getSeller, PermSettingsRead)))
	mux.HandleFunc("PUT /admin/supply/sellers/{id}", h.limit(h.perm(h.updateSeller, PermSettingsWrite)))
	mux.HandleFunc("DELETE /admin/supply/sellers/{id}", h.limit(h.perm(h.deleteSeller, PermSettingsWrite)))

	mux.HandleFunc("GET /admin/supply/ads-txt", h.limit(h.perm(h.listAdsTxtEntries, PermSettingsRead)))
	mux.HandleFunc("POST /admin/supply/ads-txt", h.limit(h.perm(h.createAdsTxtEntry, PermSettingsWrite)))
	mux.HandleFunc("GET /admin/supply/ads-txt/{id}", h.limit(h.perm(h.getAdsTxtEntry, PermSettingsRead)))
	mux.HandleFunc("PUT /admin/supply/ads-txt/{id}", h.limit(h.perm(h.updateAdsTxtEntry, PermSettingsWrite)))
	mux.HandleFunc("DELETE /admin/supply/ads-txt/{id}", h.limit(h.perm(h.deleteAdsTxtEntry, PermSettingsWrite)))

	mux.HandleFunc("GET /admin/campaigns/{id}/supply-chain", h.limit(h.perm(h.getCampaignSupplyChain, PermCampaignsRead)))
	mux.HandleFunc("PUT /admin/campaigns/{id}/supply-chain", h.limit(h.perm(h.updateCampaignSupplyChain, PermCampaignsWrite)))
}

func (h *Handler) getSellersJSON(w http.ResponseWriter, r *http.Request) {
	body, err := h.svc.GetSellersJSON(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_, _ = w.Write(body)
}

func (h *Handler) exportAdsTxt(w http.ResponseWriter, r *http.Request) {
	text, err := h.svc.BuildAdsTxt(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(text))
}

func (h *Handler) listSellers(w http.ResponseWriter, r *http.Request) {
	rows, err := h.svc.ListSellers(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]any{"sellers": rows})
}

func (h *Handler) getSeller(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid seller id")
		return
	}
	row, err := h.svc.GetSeller(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, row)
}

func (h *Handler) createSeller(w http.ResponseWriter, r *http.Request) {
	spec, err := cold.DecodeRequest[SellerCreateSpec](w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	row, err := h.svc.CreateSeller(r.Context(), spec)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusCreated, row)
}

func (h *Handler) updateSeller(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid seller id")
		return
	}
	spec, err := cold.DecodeRequest[SellerUpdateSpec](w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	row, err := h.svc.UpdateSeller(r.Context(), id, spec)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, row)
}

func (h *Handler) deleteSeller(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid seller id")
		return
	}
	if err := h.svc.DeleteSeller(r.Context(), id); err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) listAdsTxtEntries(w http.ResponseWriter, r *http.Request) {
	rows, err := h.svc.ListAdsTxtEntries(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]any{"entries": rows})
}

func (h *Handler) getAdsTxtEntry(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid entry id")
		return
	}
	row, err := h.svc.GetAdsTxtEntry(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, row)
}

func (h *Handler) createAdsTxtEntry(w http.ResponseWriter, r *http.Request) {
	spec, err := cold.DecodeRequest[AdsTxtEntryCreateSpec](w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	row, err := h.svc.CreateAdsTxtEntry(r.Context(), spec)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusCreated, row)
}

func (h *Handler) updateAdsTxtEntry(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid entry id")
		return
	}
	spec, err := cold.DecodeRequest[AdsTxtEntryUpdateSpec](w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	row, err := h.svc.UpdateAdsTxtEntry(r.Context(), id, spec)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, row)
}

func (h *Handler) deleteAdsTxtEntry(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid entry id")
		return
	}
	if err := h.svc.DeleteAdsTxtEntry(r.Context(), id); err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) getCampaignSupplyChain(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	dto, err := h.svc.GetCampaignSupplyChain(r.Context(), campaignID)
	if err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}
	httpresponse.JSON(w, http.StatusOK, dto)
}

func (h *Handler) updateCampaignSupplyChain(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	req, err := cold.DecodeRequest[struct {
		Nodes []SupplyChainNode `json:"nodes"`
	}](w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	dto, err := h.svc.UpdateCampaignSupplyChain(r.Context(), campaignID, req.Nodes)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, dto)
}

func parsePathInt64(r *http.Request, name string) (int64, error) {
	vStr := r.PathValue(name)
	return strconv.ParseInt(vStr, 10, 64)
}
