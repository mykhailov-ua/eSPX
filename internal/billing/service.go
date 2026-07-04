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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service owns invoice generation and ledger aggregation in strict pgx transactions.
type Service struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	tax     *TaxCalculator
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{
		pool:    pool,
		queries: db.New(pool),
		tax:     NewTaxCalculator(),
	}
}

// GenerateInvoice aggregates ledger spend for one customer and calendar month.
func (service *Service) GenerateInvoice(ctx context.Context, customerID uuid.UUID, billingMonth time.Time) (*pb.Invoice, error) {
	if err := validateBillingMonth(billingMonth); err != nil {
		return nil, err
	}
	if err := CheckLedgerBalanceInvariant(ctx, service.pool, customerID); err != nil {
		return nil, err
	}

	monthStart := truncateMonthUTC(billingMonth)
	monthEnd := monthStart.AddDate(0, 1, 0)

	existing, err := service.queries.GetInvoiceByCustomerMonth(ctx, db.GetInvoiceByCustomerMonthParams{
		CustomerID:   pgtype.UUID{Bytes: customerID, Valid: true},
		BillingMonth: pgtype.Date{Time: monthStart, Valid: true},
	})
	if err == nil {
		return service.invoiceToProto(ctx, existing)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("lookup invoice: %w", err)
	}

	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := service.queries.WithTx(tx)

	cust, err := qtx.GetCustomerBalance(ctx, pgtype.UUID{Bytes: customerID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrCustomerNotFound
		}
		return nil, fmt.Errorf("load customer: %w", err)
	}

	ledgerSum, err := qtx.SumCustomerLedgerTotal(ctx, pgtype.UUID{Bytes: customerID, Valid: true})
	if err != nil {
		return nil, fmt.Errorf("sum ledger: %w", err)
	}
	if diff := cust.Balance - ledgerSum; diff < -ledgerInvariantToleranceMicro || diff > ledgerInvariantToleranceMicro {
		return nil, fmt.Errorf("%w: balance=%d ledger_sum=%d diff=%d", ErrLedgerDrift, cust.Balance, ledgerSum, diff)
	}

	spendMicro, err := qtx.SumCustomerSpendInWindow(ctx, db.SumCustomerSpendInWindowParams{
		CustomerID:  pgtype.UUID{Bytes: customerID, Valid: true},
		CreatedAt:   pgTimestamp(monthStart),
		CreatedAt_2: pgTimestamp(monthEnd),
	})
	if err != nil {
		return nil, fmt.Errorf("sum spend window: %w", err)
	}

	lines, err := qtx.SumCustomerLedgerByTypeInWindow(ctx, db.SumCustomerLedgerByTypeInWindowParams{
		CustomerID:  pgtype.UUID{Bytes: customerID, Valid: true},
		CreatedAt:   pgTimestamp(monthStart),
		CreatedAt_2: pgTimestamp(monthEnd),
	})
	if err != nil {
		return nil, fmt.Errorf("aggregate ledger lines: %w", err)
	}

	profile := service.resolveTaxProfile(ctx, qtx, customerID, cust.Currency)
	taxMicro, rateBPS := service.tax.Compute(spendMicro, profile)
	totalMicro := spendMicro + taxMicro

	invoiceID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate invoice id: %w", err)
	}

	invoice, err := qtx.CreateInvoice(ctx, db.CreateInvoiceParams{
		ID:             pgtype.UUID{Bytes: invoiceID, Valid: true},
		CustomerID:     pgtype.UUID{Bytes: customerID, Valid: true},
		BillingMonth:   pgtype.Date{Time: monthStart, Valid: true},
		SubtotalMicro:  spendMicro,
		TaxMicro:       taxMicro,
		TotalMicro:     totalMicro,
		Currency:       cust.Currency,
		TaxScheme:      MapSchemeToDB(profile.Scheme),
		TaxRateBps:     rateBPS,
		LedgerSumMicro: ledgerSum,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			_ = tx.Rollback(ctx)
			existing, lookupErr := service.queries.GetInvoiceByCustomerMonth(ctx, db.GetInvoiceByCustomerMonthParams{
				CustomerID:   pgtype.UUID{Bytes: customerID, Valid: true},
				BillingMonth: pgtype.Date{Time: monthStart, Valid: true},
			})
			if lookupErr == nil {
				return service.invoiceToProto(ctx, existing)
			}
		}
		return nil, fmt.Errorf("insert invoice: %w", err)
	}

	for _, line := range lines {
		if _, err := qtx.CreateInvoiceLine(ctx, db.CreateInvoiceLineParams{
			InvoiceID:   invoice.ID,
			LedgerType:  line.LedgerType,
			AmountMicro: line.AmountMicro,
			EntryCount:  line.EntryCount,
		}); err != nil {
			return nil, fmt.Errorf("insert invoice line: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit invoice: %w", err)
	}

	return service.invoiceToProto(ctx, invoice)
}

func (service *Service) resolveTaxProfile(ctx context.Context, q *db.Queries, customerID uuid.UUID, currency string) TaxProfile {
	row, err := q.GetCustomerTaxProfile(ctx, pgtype.UUID{Bytes: customerID, Valid: true})
	if err == nil {
		return ProfileFromDB(row)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return service.tax.DefaultProfile("US", currency)
	}
	return service.tax.DefaultProfile("US", currency)
}

func (service *Service) GetInvoice(ctx context.Context, invoiceID uuid.UUID) (*pb.Invoice, error) {
	invoice, err := service.queries.GetInvoice(ctx, pgtype.UUID{Bytes: invoiceID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInvoiceNotFound
		}
		return nil, fmt.Errorf("get invoice: %w", err)
	}
	return service.invoiceToProto(ctx, invoice)
}

func (service *Service) ListInvoices(ctx context.Context, customerID uuid.UUID, limit, offset int32) ([]*pb.Invoice, int64, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	total, err := service.queries.CountCustomerInvoices(ctx, pgtype.UUID{Bytes: customerID, Valid: true})
	if err != nil {
		return nil, 0, fmt.Errorf("count invoices: %w", err)
	}

	rows, err := service.queries.ListCustomerInvoices(ctx, db.ListCustomerInvoicesParams{
		CustomerID: pgtype.UUID{Bytes: customerID, Valid: true},
		Limit:      limit,
		Offset:     offset,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("list invoices: %w", err)
	}

	invoices := make([]*pb.Invoice, 0, len(rows))
	for _, row := range rows {
		inv, err := service.invoiceToProto(ctx, row)
		if err != nil {
			return nil, 0, err
		}
		invoices = append(invoices, inv)
	}
	return invoices, total, nil
}

func (service *Service) invoiceToProto(ctx context.Context, invoice db.BillingInvoice) (*pb.Invoice, error) {
	lineRows, err := service.queries.ListInvoiceLines(ctx, invoice.ID)
	if err != nil {
		return nil, fmt.Errorf("list invoice lines: %w", err)
	}

	lines := make([]*pb.InvoiceLine, 0, len(lineRows))
	for _, line := range lineRows {
		lines = append(lines, &pb.InvoiceLine{
			LedgerType:  line.LedgerType,
			AmountMicro: line.AmountMicro,
			EntryCount:  line.EntryCount,
		})
	}

	monthTime := invoice.BillingMonth.Time.UTC()
	return &pb.Invoice{
		Id:            uuid.UUID(invoice.ID.Bytes).String(),
		CustomerId:    uuid.UUID(invoice.CustomerID.Bytes).String(),
		BillingMonth:  timestamppb.New(time.Date(monthTime.Year(), monthTime.Month(), 1, 0, 0, 0, 0, time.UTC)),
		SubtotalMicro: invoice.SubtotalMicro,
		TaxMicro:      invoice.TaxMicro,
		TotalMicro:    invoice.TotalMicro,
		Currency:      invoice.Currency,
		TaxScheme:     string(MapSchemeFromDB(invoice.TaxScheme)),
		TaxRateBps:    invoice.TaxRateBps,
		Lines:         lines,
		CreatedAt:     timestamppb.New(invoice.CreatedAt.Time),
	}, nil
}

func validateBillingMonth(month time.Time) error {
	m := month.UTC()
	if m.Day() != 1 || m.Hour() != 0 || m.Minute() != 0 || m.Second() != 0 || m.Nanosecond() != 0 {
		return ErrInvalidBillingMonth
	}
	return nil
}

func truncateMonthUTC(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func pgTimestamp(t time.Time) pgtype.Timestamp {
	return pgtype.Timestamp{Time: t.UTC(), Valid: true}
}

// ParseBillingMonth parses YYYY-MM into the first day of that month in UTC.
func ParseBillingMonth(raw string) (time.Time, error) {
	t, err := time.Parse("2006-01", raw)
	if err != nil {
		return time.Time{}, ErrInvalidBillingMonth
	}
	return truncateMonthUTC(t), nil
}
