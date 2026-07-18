package billing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"espx/internal/billing/db"
	"espx/internal/billing/pb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// InvoicePreview is the dry-run result for POST /api/v1/billing/invoices/preview.
type InvoicePreview struct {
	CustomerID     string           `json:"customer_id"`
	BillingMonth   string           `json:"billing_month"`
	Currency       string           `json:"currency"`
	SubtotalMicro  int64            `json:"subtotal_micro"`
	TaxMicro       int64            `json:"tax_micro"`
	TotalMicro     int64            `json:"total_micro"`
	TaxScheme      string           `json:"tax_scheme"`
	TaxRateBps     int32            `json:"tax_rate_bps"`
	Lines          []InvoiceLineDTO `json:"lines"`
	WouldSkip      bool             `json:"would_skip"`
	LedgerSumMicro int64            `json:"ledger_sum_micro"`
}

// InvoiceLineDTO is one aggregated ledger line in preview/statement views.
type InvoiceLineDTO struct {
	LedgerType  string `json:"ledger_type"`
	AmountMicro int64  `json:"amount_micro"`
	EntryCount  int32  `json:"entry_count"`
}

// PreviewInvoice computes invoice lines without persisting a row.
func (service *Service) PreviewInvoice(ctx context.Context, customerID uuid.UUID, billingMonth time.Time) (*InvoicePreview, error) {
	if err := validateBillingMonth(billingMonth); err != nil {
		return nil, err
	}
	if err := CheckLedgerBalanceInvariant(ctx, service.pool, customerID); err != nil {
		return nil, err
	}

	monthStart := truncateMonthUTC(billingMonth)
	monthEnd := monthStart.AddDate(0, 1, 0)

	cust, err := service.queries.GetCustomerBalance(ctx, pgtype.UUID{Bytes: customerID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrCustomerNotFound
		}
		return nil, err
	}

	ledgerSum, err := service.queries.SumCustomerLedgerTotal(ctx, pgtype.UUID{Bytes: customerID, Valid: true})
	if err != nil {
		return nil, err
	}

	spendMicro, err := service.queries.SumCustomerSpendInWindow(ctx, db.SumCustomerSpendInWindowParams{
		CustomerID:  pgtype.UUID{Bytes: customerID, Valid: true},
		CreatedAt:   pgTimestamp(monthStart),
		CreatedAt_2: pgTimestamp(monthEnd),
	})
	if err != nil {
		return nil, err
	}

	lines, err := service.queries.SumCustomerLedgerByTypeInWindow(ctx, db.SumCustomerLedgerByTypeInWindowParams{
		CustomerID:  pgtype.UUID{Bytes: customerID, Valid: true},
		CreatedAt:   pgTimestamp(monthStart),
		CreatedAt_2: pgTimestamp(monthEnd),
	})
	if err != nil {
		return nil, err
	}

	profile := service.resolveTaxProfile(ctx, service.queries, customerID, cust.Currency)
	taxMicro, rateBPS := service.tax.Compute(spendMicro, profile)
	totalMicro := spendMicro + taxMicro

	out := &InvoicePreview{
		CustomerID:     customerID.String(),
		BillingMonth:   monthStart.Format("2006-01"),
		Currency:       cust.Currency,
		SubtotalMicro:  spendMicro,
		TaxMicro:       taxMicro,
		TotalMicro:     totalMicro,
		TaxScheme:      string(profile.Scheme),
		TaxRateBps:     rateBPS,
		LedgerSumMicro: ledgerSum,
		WouldSkip:      spendMicro == 0,
	}
	for _, line := range lines {
		out.Lines = append(out.Lines, InvoiceLineDTO{
			LedgerType:  line.LedgerType,
			AmountMicro: line.AmountMicro,
			EntryCount:  line.EntryCount,
		})
	}
	return out, nil
}

// PreviewInvoiceProto exposes preview over gRPC-shaped clients.
func (service *Service) PreviewInvoiceProto(ctx context.Context, customerID uuid.UUID, billingMonth time.Time) (*pb.Invoice, bool, error) {
	preview, err := service.PreviewInvoice(ctx, customerID, billingMonth)
	if err != nil {
		return nil, false, err
	}
	if preview.WouldSkip {
		return nil, true, nil
	}
	month, _ := time.Parse("2006-01", preview.BillingMonth)
	lines := make([]*pb.InvoiceLine, 0, len(preview.Lines))
	for _, l := range preview.Lines {
		lines = append(lines, &pb.InvoiceLine{
			LedgerType:  l.LedgerType,
			AmountMicro: l.AmountMicro,
			EntryCount:  l.EntryCount,
		})
	}
	return &pb.Invoice{
		CustomerId:    preview.CustomerID,
		BillingMonth:  timestamppb.New(month),
		SubtotalMicro: preview.SubtotalMicro,
		TaxMicro:      preview.TaxMicro,
		TotalMicro:    preview.TotalMicro,
		Currency:      preview.Currency,
		TaxScheme:     preview.TaxScheme,
		TaxRateBps:    preview.TaxRateBps,
		Lines:         lines,
	}, false, nil
}

// VoidInvoice marks a finalized invoice as VOID without mutating the ledger.
func (service *Service) VoidInvoice(ctx context.Context, invoiceID uuid.UUID) error {
	tag, err := service.queries.VoidInvoice(ctx, pgtype.UUID{Bytes: invoiceID, Valid: true})
	if err != nil {
		return err
	}
	if tag == 0 {
		inv, lookupErr := service.queries.GetInvoice(ctx, pgtype.UUID{Bytes: invoiceID, Valid: true})
		if lookupErr != nil {
			if errors.Is(lookupErr, pgx.ErrNoRows) {
				return ErrInvoiceNotFound
			}
			return lookupErr
		}
		if inv.Status == db.BillingInvoiceStatusVOID {
			return nil
		}
		return fmt.Errorf("invoice cannot be voided in status %s", inv.Status)
	}
	return nil
}
