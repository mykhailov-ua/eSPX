package adminapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"espx/pkg/coldpath"
	"espx/pkg/httpresponse"
	"espx/pkg/money"

	"github.com/google/uuid"
)

const (
	forecastHandlerTimeout       = 2 * time.Second
	forecastDefaultRetryAfterSec = 30
)

// CampaignForecaster estimates delivery for a planned campaign.
type CampaignForecaster interface {
	ForecastCampaign(ctx context.Context, in CampaignForecastInput) (CampaignForecastDTO, error)
}

// ForecastRetryAfterSec returns the Retry-After hint for forecast 503 responses.
func ForecastRetryAfterSec() int {
	return forecastDefaultRetryAfterSec
}

func (h *ReportsHTTPHandlers) registerCampaignForecast(mux *http.ServeMux) {
	if h.CampaignForecaster == nil {
		return
	}
	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	mux.HandleFunc("POST /api/v1/forecast/campaign", limit(perm("campaigns:read", h.forecastCampaign)))
}

func (h *ReportsHTTPHandlers) forecastCampaign(w http.ResponseWriter, r *http.Request) {
	body, err := coldpath.ReadLimitedBody(w, r, coldpath.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to read request body")
		return
	}

	req, err := coldpath.DecodeBody[struct {
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
		h.writeServiceError(w, err)
		return
	}

	budgetLegacy := 0.0
	hasLegacy := req.BudgetLimit != nil
	if hasLegacy {
		budgetLegacy = *req.BudgetLimit
	}
	budgetMicro, err := forecastParseBudgetMicro(req.BudgetLimitMicro, budgetLegacy, hasLegacy)
	if err != nil {
		h.writeServiceError(w, err)
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

	out, err := h.CampaignForecaster.ForecastCampaign(ctx, CampaignForecastInput{
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
		WriteForecastError(w, err)
		return
	}

	httpresponse.JSON(w, http.StatusOK, out)
}

func (h *ReportsHTTPHandlers) resolveForecastCustomerID(r *http.Request, bodyCustomerID *uuid.UUID) (*uuid.UUID, error) {
	if h.ResolveForecastCustomerID == nil {
		return bodyCustomerID, nil
	}
	return h.ResolveForecastCustomerID(r, bodyCustomerID)
}

func forecastParseBudgetMicro(micro *int64, legacy float64, hasLegacy bool) (int64, error) {
	if micro != nil {
		if *micro <= 0 {
			return 0, errInvalidQuery("budget must be positive")
		}
		return *micro, nil
	}
	if hasLegacy {
		v, err := money.LegacyFloatToMicro(legacy)
		if err != nil || v <= 0 {
			return 0, errInvalidQuery("budget must be positive")
		}
		return v, nil
	}
	return 0, errInvalidQuery("budget is required")
}

func WriteForecastError(w http.ResponseWriter, err error) {
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
	httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
}
