package management

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"espx/pkg/cold"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

const forecastHandlerTimeout = 2 * time.Second

// forecastCampaign handles POST /api/v1/forecast/campaign (M5.1).
func (h *Handler) forecastCampaign(w http.ResponseWriter, r *http.Request) {
	body, err := cold.ReadLimitedBody(w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to read request body")
		return
	}

	req, err := cold.DecodeBody[struct {
		CustomerID       *uuid.UUID `json:"customer_id,omitempty"`
		BudgetLimitMicro *int64     `json:"budget_limit_micro"`
		BudgetLimit      *float64   `json:"budget_limit"`
		TargetCountries  []string   `json:"target_countries"`
		DaypartHours     []int16    `json:"daypart_hours"`
		StartAt          *time.Time `json:"start_at"`
		EndAt            *time.Time `json:"end_at"`
		PacingMode       string     `json:"pacing_mode"`
		Timezone         string     `json:"timezone"`
	}](body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}

	customerID, err := h.resolveForecastCustomerID(r, req.CustomerID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	budgetLegacy := 0.0
	hasLegacy := req.BudgetLimit != nil
	if hasLegacy {
		budgetLegacy = *req.BudgetLimit
	}
	budgetMicro, err := parseBudgetMicro(req.BudgetLimitMicro, budgetLegacy, hasLegacy)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	if req.StartAt == nil || req.EndAt == nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "start_at and end_at are required")
		return
	}

	tz := req.Timezone
	if tz == "" {
		tz = "UTC"
	}

	ctx, cancel := context.WithTimeout(r.Context(), forecastHandlerTimeout)
	defer cancel()

	out, err := h.svc.ForecastCampaign(ctx, CampaignForecastInput{
		CustomerID:       customerID,
		BudgetLimitMicro: budgetMicro,
		TargetCountries:  req.TargetCountries,
		DaypartHours:     req.DaypartHours,
		StartAt:          req.StartAt.UTC(),
		EndAt:            req.EndAt.UTC(),
		PacingMode:       req.PacingMode,
		Timezone:         tz,
	})
	if err != nil {
		writeForecastError(w, err)
		return
	}

	httpresponse.JSON(w, http.StatusOK, out)
}

func (h *Handler) resolveForecastCustomerID(r *http.Request, bodyCustomerID *uuid.UUID) (*uuid.UUID, error) {
	u, ok := GetUser(r.Context())
	if !ok {
		return nil, errForbidden
	}
	if u.IsUser() {
		if bodyCustomerID != nil && *bodyCustomerID != uuid.Nil && *bodyCustomerID != u.CustomerID {
			return nil, errForbidden
		}
		cid := u.CustomerID
		return &cid, nil
	}
	if bodyCustomerID != nil && *bodyCustomerID != uuid.Nil {
		return bodyCustomerID, nil
	}
	return nil, nil
}

func writeForecastError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrForecastClickHouseTimeout) || errors.Is(err, ErrForecastUnavailable) {
		w.Header().Set("Retry-After", strconv.Itoa(ForecastRetryAfterSec()))
		httpresponse.JSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": map[string]string{
				"code":    "FORECAST_UNAVAILABLE",
				"message": err.Error(),
			},
			"retry_after": ForecastRetryAfterSec(),
		})
		return
	}
	if errors.Is(err, ErrClickHouseNotConfigured) {
		httpresponse.Error(w, http.StatusServiceUnavailable, "CLICKHOUSE_UNAVAILABLE", "clickhouse not configured")
		return
	}
	writeServiceError(w, err)
}
