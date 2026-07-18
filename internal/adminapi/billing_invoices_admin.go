package adminapi

import (
	"context"
	"fmt"
	"time"

	billingdb "espx/internal/billing/db"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgtype"
)

// AdminInvoiceFilters are optional query filters for cross-customer invoice list.
type AdminInvoiceFilters struct {
	CustomerID *uuid.UUID
	Month      *time.Time
	Status     string
	MinTotal   int64
}

// AdminInvoiceListResult is the paginated admin invoice list response.
type AdminInvoiceListResult struct {
	Items  []InvoiceSummaryDTO `json:"items"`
	Total  int64               `json:"total"`
	Limit  int32               `json:"limit"`
	Offset int32               `json:"offset"`
}

// ListInvoicesAdmin returns invoices across customers with optional filters (admin only).
func (s *CompositeReadService) ListInvoicesAdmin(ctx context.Context, filters AdminInvoiceFilters, limit, offset int32) (AdminInvoiceListResult, error) {
	if s == nil || s.queries == nil {
		return AdminInvoiceListResult{}, fmt.Errorf("composite read service not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}

	var customer pgtype.UUID
	if filters.CustomerID != nil {
		customer = pgtype.UUID{Bytes: *filters.CustomerID, Valid: true}
	}
	var month pgtype.Date
	if filters.Month != nil {
		m := filters.Month.UTC()
		month = pgtype.Date{Time: time.Date(m.Year(), m.Month(), m.Day(), 0, 0, 0, 0, time.UTC), Valid: true}
	}

	params := billingdb.ListInvoicesAdminParams{
		Column1: customer,
		Column2: month,
		Column3: filters.Status,
		Column4: filters.MinTotal,
		Limit:   limit,
		Offset:  offset,
	}
	rows, err := s.queries.ListInvoicesAdmin(ctx, params)
	if err != nil {
		return AdminInvoiceListResult{}, err
	}
	total, err := s.queries.CountInvoicesAdmin(ctx, billingdb.CountInvoicesAdminParams{
		Column1: customer,
		Column2: month,
		Column3: filters.Status,
		Column4: filters.MinTotal,
	})
	if err != nil {
		return AdminInvoiceListResult{}, err
	}

	items := make([]InvoiceSummaryDTO, 0, len(rows))
	for _, inv := range rows {
		monthStr := ""
		if inv.BillingMonth.Valid {
			monthStr = inv.BillingMonth.Time.UTC().Format("2006-01")
		}
		items = append(items, InvoiceSummaryDTO{
			ID:            uuidString(inv.ID),
			CustomerID:    uuidString(inv.CustomerID),
			BillingMonth:  monthStr,
			SubtotalMicro: inv.SubtotalMicro,
			TaxMicro:      inv.TaxMicro,
			TotalMicro:    inv.TotalMicro,
			Status:        string(inv.Status),
			Currency:      inv.Currency,
		})
	}
	return AdminInvoiceListResult{
		Items:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}, nil
}
