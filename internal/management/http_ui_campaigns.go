package management

import (
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"

	"espx/internal/ads/db"
	"espx/pkg/httpresponse"
	uicampaigns "espx/ui/campaigns"

	"github.com/google/uuid"
)

const defaultGrafanaURL = "http://127.0.0.1:3100"

func (h *UIHandler) campaignsList(w http.ResponseWriter, r *http.Request) {
	user, _ := GetUser(r.Context())
	custID := uuid.Nil
	if user.IsUser() {
		custID = user.CustomerID
	}

	campaigns, total, err := h.svc.ListCampaigns(r.Context(), custID, "", 50, 0)
	if err != nil {
		slog.Error("failed to list campaigns for ui", "error", err)
		httpresponse.HTMXError(w, r, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "failed to load campaigns")
		return
	}

	rows := make([]uicampaigns.Row, len(campaigns))
	for i, c := range campaigns {
		rows[i] = uicampaigns.Row{
			ID:           c.ID,
			Name:         c.Name,
			Status:       c.Status,
			BudgetLimit:  c.BudgetLimit,
			CurrentSpend: c.CurrentSpend,
			PacingMode:   c.PacingMode,
			CustomerID:   c.CustomerID,
		}
	}

	page := uicampaigns.ListPage{
		CSRF:       csrfFromRequest(r),
		UserEmail:  userDisplayEmail(r, user),
		UserRole:   user.Role,
		GrafanaURL: h.grafanaURL,
		Campaigns:  rows,
		Total:      total,
	}
	if ok := r.URL.Query().Get("ok"); ok == "created" {
		page.FlashOK = "Campaign created."
	} else if ok == "cancelled" {
		page.FlashOK = "Campaign cancellation started."
	}
	if errMsg := r.URL.Query().Get("error"); errMsg != "" {
		page.FlashError = errMsg
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := uicampaigns.List(page).Render(r.Context(), w); err != nil {
		slog.Error("failed to render campaigns list", "error", err)
	}
}

func (h *UIHandler) campaignsNew(w http.ResponseWriter, r *http.Request) {
	page, err := h.newCampaignPage(r, uicampaigns.NewForm{}, "")
	if err != nil {
		slog.Error("failed to build new campaign page", "error", err)
		httpresponse.HTMXError(w, r, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal error")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uicampaigns.New(page).Render(r.Context(), w); err != nil {
		slog.Error("failed to render new campaign page", "error", err)
	}
}

func (h *UIHandler) campaignsCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderNewCampaignError(w, r, uicampaigns.NewForm{}, "invalid form")
		return
	}

	form := uicampaigns.NewForm{
		CustomerID:      r.FormValue("customer_id"),
		Name:            strings.TrimSpace(r.FormValue("name")),
		BudgetLimit:     strings.TrimSpace(r.FormValue("budget_limit")),
		PacingMode:      r.FormValue("pacing_mode"),
		Timezone:        strings.TrimSpace(r.FormValue("timezone")),
		TargetCountries: strings.TrimSpace(r.FormValue("target_countries")),
	}

	customerID, err := uuid.Parse(form.CustomerID)
	if err != nil {
		h.renderNewCampaignError(w, r, form, "customer is required")
		return
	}
	if form.Name == "" {
		h.renderNewCampaignError(w, r, form, "name is required")
		return
	}

	budget, err := strconv.ParseFloat(form.BudgetLimit, 64)
	if err != nil || budget <= 0 || math.IsNaN(budget) {
		h.renderNewCampaignError(w, r, form, "budget must be a positive number")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() && customerID != u.CustomerID {
		h.renderNewCampaignError(w, r, form, "forbidden: cannot create campaign for another customer")
		return
	}

	pacing := db.PacingModeTypeASAP
	if form.PacingMode == "EVEN" {
		pacing = db.PacingModeTypeEVEN
	}
	tz := form.Timezone
	if tz == "" {
		tz = "UTC"
	}

	var countries []string
	if form.TargetCountries != "" {
		for _, part := range strings.Split(form.TargetCountries, ",") {
			c := strings.TrimSpace(strings.ToUpper(part))
			if c != "" {
				countries = append(countries, c)
			}
		}
	}

	body, _ := json.Marshal(map[string]any{
		"customer_id":  customerID,
		"name":         form.Name,
		"budget_limit": budget,
		"pacing_mode":  form.PacingMode,
		"timezone":     tz,
	})
	hash := h.svc.GenerateIdempotencyHash(customerID, body)

	_, err = h.svc.CreateCampaign(r.Context(), CampaignCreateSpec{
		CustomerID:      customerID,
		Name:            form.Name,
		BudgetLimit:     int64(math.Round(budget * microUnitFactor)),
		PacingMode:      pacing,
		Timezone:        tz,
		TargetCountries: countries,
		FreqWindow:      86400,
		IdempotencyKey:  hash,
	})
	if err != nil {
		slog.Warn("ui create campaign failed", "error", err)
		h.renderNewCampaignError(w, r, form, "failed to create campaign: "+err.Error())
		return
	}

	http.Redirect(w, r, "/admin/campaigns/manage?ok=created", http.StatusSeeOther)
}

func (h *UIHandler) campaignsCancel(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		http.Redirect(w, r, "/admin/campaigns/manage?error=invalid+campaign+id", http.StatusSeeOther)
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			http.Redirect(w, r, "/admin/campaigns/manage?error=forbidden", http.StatusSeeOther)
			return
		}
	}

	if err := h.svc.CancelCampaign(r.Context(), campaignID, "ui cancel"); err != nil {
		slog.Warn("ui cancel campaign failed", "campaign_id", campaignID, "error", err)
		http.Redirect(w, r, "/admin/campaigns/manage?error=cancel+failed", http.StatusSeeOther)
		return
	}

	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", "/admin/campaigns/manage?ok=cancelled")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/admin/campaigns/manage?ok=cancelled", http.StatusSeeOther)
}

func (h *UIHandler) dashboardRedirect(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/admin/campaigns/manage", http.StatusSeeOther)
}

func (h *UIHandler) newCampaignPage(r *http.Request, form uicampaigns.NewForm, errMsg string) (uicampaigns.NewPage, error) {
	user, _ := GetUser(r.Context())

	var customers []CustomerDTO
	if !user.IsUser() {
		var err error
		customers, _, err = h.svc.ListCustomers(r.Context(), 100, 0)
		if err != nil {
			return uicampaigns.NewPage{}, err
		}
	} else {
		form.CustomerID = user.CustomerID.String()
	}

	opts := make([]uicampaigns.CustomerOption, len(customers))
	for i, c := range customers {
		opts[i] = uicampaigns.CustomerOption{ID: c.ID, Name: c.Name}
	}

	return uicampaigns.NewPage{
		CSRF:       csrfFromRequest(r),
		UserEmail:  userDisplayEmail(r, user),
		UserRole:   user.Role,
		GrafanaURL: h.grafanaURL,
		Customers:  opts,
		Error:      errMsg,
		Form:       form,
	}, nil
}

func (h *UIHandler) renderNewCampaignError(w http.ResponseWriter, r *http.Request, form uicampaigns.NewForm, message string) {
	page, err := h.newCampaignPage(r, form, message)
	if err != nil {
		httpresponse.HTMXError(w, r, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal error")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = uicampaigns.New(page).Render(r.Context(), w)
}
