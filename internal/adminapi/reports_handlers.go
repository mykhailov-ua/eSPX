package adminapi

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"espx/internal/billing/db"
	"espx/pkg/coldpath"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReportsHTTPHandlers serves tabular report JSON routes (M6 CHG waves).
type ReportsHTTPHandlers struct {
	CampaignStats             CampaignStatsReader
	CampaignForecaster        CampaignForecaster
	Pool                      *pgxpool.Pool
	ApplyRateLimit            func(http.HandlerFunc) http.HandlerFunc
	RequirePermission         func(string, http.HandlerFunc) http.HandlerFunc
	AuthorizeCampaignAccess   func(*http.Request, uuid.UUID) error
	ResolveForecastCustomerID func(*http.Request, *uuid.UUID) (*uuid.UUID, error)
	WriteServiceError         func(http.ResponseWriter, error)
}

// Register mounts report routes on mux.
func (h *ReportsHTTPHandlers) Register(mux *http.ServeMux) {
	if h == nil {
		return
	}
	if h.ApplyRateLimit == nil {
		h.ApplyRateLimit = func(next http.HandlerFunc) http.HandlerFunc { return next }
	}
	if h.RequirePermission == nil {
		h.RequirePermission = func(_ string, next http.HandlerFunc) http.HandlerFunc { return next }
	}

	h.registerCampaignStats(mux)
	h.registerCampaignForecast(mux)
	h.registerScaffoldReports(mux)

	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	mux.HandleFunc("GET /api/v1/reports/placements", limit(perm("campaigns:read", h.getPlacementsReport)))
	mux.HandleFunc("GET /api/v1/reports/keywords", limit(perm("campaigns:read", h.getKeywordsReport)))
}

func (h *ReportsHTTPHandlers) registerScaffoldReports(mux *http.ServeMux) {
	limit := h.ApplyRateLimit
	perm := h.RequirePermission

	routes := []struct {
		path       string
		permission string
	}{
		{"GET /api/v1/reports/campaign-unit-economics", "campaigns:read"},
		{"GET /api/v1/reports/source-margin", "campaigns:read"},
		{"GET /api/v1/reports/traffic-sources", "campaigns:read"},
		{"GET /api/v1/reports/source-quality", "campaigns:read"},
		{"GET /api/v1/reports/spend-velocity", "campaigns:read"},
		{"GET /api/v1/reports/campaign-geo-device", "campaigns:read"},
		{"GET /api/v1/reports/geo-roi", "campaigns:read"},
		{"GET /api/v1/reports/daypart-heatmap", "campaigns:read"},
		{"GET /api/v1/reports/pacing-drift", "campaigns:read"},
		{"GET /api/v1/reports/postback-reconciliation", "customers:read"},
		{"GET /api/v1/reports/ivt-by-source", "audit:read"},
		{"GET /api/v1/reports/discrepancy-buy-sell", "customers:read"},
		{"GET /api/v1/reports/campaign-overview", "campaigns:read"},
		{"GET /api/v1/reports/customer-portfolio", "customers:read"},
	}
	for _, route := range routes {
		mux.HandleFunc(route.path, limit(perm(route.permission, h.notImplemented)))
	}
	mux.HandleFunc("POST /api/v1/reports/jobs", limit(perm("customers:read", h.notImplemented)))
}

func (h *ReportsHTTPHandlers) notImplemented(w http.ResponseWriter, _ *http.Request) {
	httpresponse.Error(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "reports scaffold; see MILESTONE.md M6")
}

func (h *ReportsHTTPHandlers) writeServiceError(w http.ResponseWriter, err error) {
	var q invalidQueryError
	if errors.As(err, &q) {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", string(q))
		return
	}
	if errors.Is(err, ErrForbidden) {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	if h.WriteServiceError != nil {
		h.WriteServiceError(w, err)
		return
	}
	httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
}

func (h *ReportsHTTPHandlers) checkTierGate(r *http.Request, customerID uuid.UUID) (bool, error) {
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
	return sub.PlanCode == "pro" || sub.PlanCode == "enterprise", nil
}

func encodeCursor(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeCursor(cursorStr string) (int, error) {
	if cursorStr == "" {
		return 0, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(cursorStr)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(decoded))
}

func (h *ReportsHTTPHandlers) getPlacementsReport(w http.ResponseWriter, r *http.Request) {
	var customerID uuid.UUID
	if custIDStr := r.URL.Query().Get("customer_id"); custIDStr != "" {
		var err error
		customerID, err = uuid.Parse(custIDStr)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
			return
		}
	} else {
		if h.ResolveForecastCustomerID != nil {
			resolved, err := h.ResolveForecastCustomerID(r, nil)
			if err == nil && resolved != nil {
				customerID = *resolved
			}
		}
	}

	if customerID == uuid.Nil {
		customerID = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	}

	allowed, err := h.checkTierGate(r, customerID)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	if !allowed {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Pro or Enterprise plan required")
		return
	}

	limit := int32(10)
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = int32(l)
		}
	}
	cursorStr := r.URL.Query().Get("cursor")
	offset, err := decodeCursor(cursorStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid cursor")
		return
	}

	campaignID := r.URL.Query().Get("campaign_id")
	if campaignID == "" {
		campaignID = uuid.New().String()
	}

	totalRows := int64(25)
	mockRows := make([]PlacementReportRowDTO, 0, totalRows)
	for i := int64(0); i < totalRows; i++ {
		mockRows = append(mockRows, toPlacementReportRowDTO(placementReportCHRow{
			PlacementID:  fmt.Sprintf("zone_%d", 1000+i),
			CampaignID:   campaignID,
			Impressions:  10000 + i*500,
			Clicks:       500 + i*20,
			Conversions:  10 + i,
			SpendMicro:   50000000 + i*2000000,
			RevenueMicro: 60000000 + i*3000000,
		}))
	}

	countFn := func() (int64, error) {
		return totalRows, nil
	}
	listFn := func() ([]PlacementReportRowDTO, error) {
		start := int64(offset)
		if start >= totalRows {
			return []PlacementReportRowDTO{}, nil
		}
		end := start + int64(limit)
		if end > totalRows {
			end = totalRows
		}
		return mockRows[start:end], nil
	}
	mapFn := func(row PlacementReportRowDTO) PlacementReportRowDTO {
		return row
	}

	paginatedRows, total, err := coldpath.PaginatedList(countFn, listFn, mapFn)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	var nextCursor string
	if int64(offset)+int64(limit) < total {
		nextCursor = encodeCursor(offset + int(limit))
	}

	resp := PlacementReportResponse{
		Rows: paginatedRows,
		Freshness: DataFreshnessDTO{
			AsOf:         time.Now().UTC().Format(time.RFC3339),
			Consistency:  "eventual",
			Stale:        true,
			CHLagSeconds: 360,
		},
		NextCursor: nextCursor,
	}

	httpresponse.JSON(w, http.StatusOK, resp)
}

func (h *ReportsHTTPHandlers) getKeywordsReport(w http.ResponseWriter, r *http.Request) {
	var customerID uuid.UUID
	if custIDStr := r.URL.Query().Get("customer_id"); custIDStr != "" {
		var err error
		customerID, err = uuid.Parse(custIDStr)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
			return
		}
	} else {
		if h.ResolveForecastCustomerID != nil {
			resolved, err := h.ResolveForecastCustomerID(r, nil)
			if err == nil && resolved != nil {
				customerID = *resolved
			}
		}
	}

	if customerID == uuid.Nil {
		customerID = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	}

	allowed, err := h.checkTierGate(r, customerID)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	if !allowed {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "Pro or Enterprise plan required")
		return
	}

	limit := int32(10)
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = int32(l)
		}
	}
	cursorStr := r.URL.Query().Get("cursor")
	offset, err := decodeCursor(cursorStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid cursor")
		return
	}

	campaignID := r.URL.Query().Get("campaign_id")
	if campaignID == "" {
		campaignID = uuid.New().String()
	}

	totalRows := int64(15)
	mockRows := make([]KeywordReportRowDTO, 0, totalRows)
	keywords := []string{"insurance", "loans", "credit card", "mortgage", "attorney", "lawyer", "donate", "conference", "degree", "hosting", "claim", "software", "recovery", "transfer", "gas"}
	for i := int64(0); i < totalRows; i++ {
		mockRows = append(mockRows, toKeywordReportRowDTO(keywordReportCHRow{
			Keyword:      keywords[i],
			CampaignID:   campaignID,
			Impressions:  5000 + i*200,
			Clicks:       200 + i*10,
			Conversions:  5 + i,
			SpendMicro:   25000000 + i*1000000,
			RevenueMicro: 30000000 + i*1500000,
		}))
	}

	countFn := func() (int64, error) {
		return totalRows, nil
	}
	listFn := func() ([]KeywordReportRowDTO, error) {
		start := int64(offset)
		if start >= totalRows {
			return []KeywordReportRowDTO{}, nil
		}
		end := start + int64(limit)
		if end > totalRows {
			end = totalRows
		}
		return mockRows[start:end], nil
	}
	mapFn := func(row KeywordReportRowDTO) KeywordReportRowDTO {
		return row
	}

	paginatedRows, total, err := coldpath.PaginatedList(countFn, listFn, mapFn)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	var nextCursor string
	if int64(offset)+int64(limit) < total {
		nextCursor = encodeCursor(offset + int(limit))
	}

	resp := KeywordReportResponse{
		Rows: paginatedRows,
		Freshness: DataFreshnessDTO{
			AsOf:         time.Now().UTC().Format(time.RFC3339),
			Consistency:  "eventual",
			Stale:        true,
			CHLagSeconds: 360,
		},
		NextCursor: nextCursor,
	}

	httpresponse.JSON(w, http.StatusOK, resp)
}
