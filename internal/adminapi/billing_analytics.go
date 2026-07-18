package adminapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"espx/internal/billing"
	billingdb "espx/internal/billing/db"

	"espx/internal/config"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jackc/pgx/v5/pgxpool"
)

const ledgerInvariantToleranceMicro = int64(1)

// CompositeReadService runs composite billing reads for admin JSON endpoints.
type CompositeReadService struct {
	pool     *pgxpool.Pool
	cfg      *config.Config
	provider billing.PaymentProvider
	queries  *billingdb.Queries
	ch       driver.Conn
}

// NewCompositeReadService constructs statement and wallet read models.
func NewCompositeReadService(pool *pgxpool.Pool, cfg *config.Config, provider billing.PaymentProvider) *CompositeReadService {
	if pool == nil {
		return nil
	}
	return &CompositeReadService{
		pool:     pool,
		cfg:      cfg,
		provider: provider,
		queries:  billingdb.New(pool),
	}
}

// PeriodBounds is the resolved UTC window for statement queries.
type PeriodBounds struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// StatementDTO is the JOIN-06 billing statement read model.
type StatementDTO struct {
	CustomerID          string                   `json:"customer_id"`
	Period              PeriodBounds             `json:"period"`
	OpeningBalanceMicro int64                    `json:"opening_balance_micro"`
	ClosingBalanceMicro int64                    `json:"closing_balance_micro"`
	Lines               []billing.InvoiceLineDTO `json:"lines"`
	Invoices            []InvoiceSummaryDTO      `json:"invoices"`
	Payments            []PaymentSummaryDTO      `json:"payments"`
	TaxBreakdown        TaxBreakdownDTO          `json:"tax_breakdown"`
	Reconciliation      ReconciliationDTO        `json:"reconciliation"`
	Currency            string                   `json:"currency"`
}

// InvoiceSummaryDTO is a compact invoice row in a statement period.
type InvoiceSummaryDTO struct {
	ID            string `json:"id"`
	CustomerID    string `json:"customer_id,omitempty"`
	BillingMonth  string `json:"billing_month"`
	SubtotalMicro int64  `json:"subtotal_micro"`
	TaxMicro      int64  `json:"tax_micro"`
	TotalMicro    int64  `json:"total_micro"`
	Status        string `json:"status"`
	Currency      string `json:"currency"`
}

// PaymentSummaryDTO is a top-up row in a statement period.
type PaymentSummaryDTO struct {
	LedgerID        int64  `json:"ledger_id"`
	AmountMicro     int64  `json:"amount_micro"`
	PaymentIntentID string `json:"payment_intent_id,omitempty"`
	CreatedAt       string `json:"created_at"`
}

// TaxBreakdownDTO summarizes tax in the statement window.
type TaxBreakdownDTO struct {
	Scheme   string `json:"scheme"`
	RateBps  int32  `json:"rate_bps"`
	TaxMicro int64  `json:"tax_micro"`
}

// ReconciliationDTO compares invoice totals to ledger movement.
type ReconciliationDTO struct {
	InvoiceTotalMicro int64 `json:"invoice_total_micro"`
	LedgerSumMicro    int64 `json:"ledger_sum_micro"`
	DeltaMicro        int64 `json:"delta_micro"`
}

// WalletDTO is the wallet card for GET /api/v1/customers/{id}/wallet.
type WalletDTO struct {
	CustomerID                string `json:"customer_id"`
	BalanceMicro              int64  `json:"balance_micro"`
	Currency                  string `json:"currency"`
	AllowedOverdraftMicro     int64  `json:"allowed_overdraft_micro"`
	LowBalanceThresholdMicro  int64  `json:"low_balance_threshold_micro"`
	BurnDaysEstimate          *int   `json:"burn_days_estimate,omitempty"`
	LastInvoiceAt             string `json:"last_invoice_at,omitempty"`
	PaymentProvider           string `json:"payment_provider"`
	PaymentProviderConfigured bool   `json:"payment_provider_configured"`
}

// InvariantDTO is the FIN-07 ledger invariant HTTP response.
type InvariantDTO struct {
	OK             bool   `json:"ok"`
	CustomerID     string `json:"customer_id,omitempty"`
	BalanceMicro   int64  `json:"balance_micro,omitempty"`
	LedgerSumMicro int64  `json:"ledger_sum_micro,omitempty"`
	DiffMicro      int64  `json:"diff_micro,omitempty"`
}

// SummaryDTO is the ops billing dashboard aggregate.
type SummaryDTO struct {
	InvoicedMTDMicro                int64 `json:"invoiced_mtd_micro"`
	InvoiceCountMTD                 int64 `json:"invoice_count_mtd"`
	UndeliveredInvoiceNotifications int64 `json:"undelivered_invoice_notifications"`
	CustomersWithSpendInMonth       int64 `json:"customers_with_spend_in_month"`
}

// DeliveryDTO is one notifier row for an invoice.
type DeliveryDTO struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	Provider     string `json:"provider"`
	Recipient    string `json:"recipient"`
	TemplateID   string `json:"template_id"`
	ErrorMessage string `json:"error_message,omitempty"`
	RetryCount   int32  `json:"retry_count"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// LedgerLineDTO is one balance_ledger row for invoice drill-down.
type LedgerLineDTO struct {
	ID          int64  `json:"id"`
	AmountMicro int64  `json:"amount_micro"`
	LedgerType  string `json:"ledger_type"`
	CreatedAt   string `json:"created_at"`
}

// TaxProfileDTO is the customer tax profile JSON shape.
type TaxProfileDTO struct {
	CustomerID  string `json:"customer_id"`
	CountryCode string `json:"country_code"`
	TaxRegion   string `json:"tax_region,omitempty"`
	TaxScheme   string `json:"tax_scheme"`
	TaxRateBps  int32  `json:"tax_rate_bps"`
}

// BuildStatement assembles opening/closing balances and period activity.
func (s *CompositeReadService) BuildStatement(ctx context.Context, customerID uuid.UUID, from, to time.Time) (StatementDTO, error) {
	if s == nil || s.pool == nil {
		return StatementDTO{}, fmt.Errorf("composite read service not configured")
	}
	from = from.UTC()
	to = to.UTC()
	if !to.After(from) {
		return StatementDTO{}, fmt.Errorf("invalid period: to must be after from")
	}

	pgCustomer := pgtype.UUID{Bytes: customerID, Valid: true}
	opening, err := s.queries.SumCustomerLedgerBefore(ctx, billingdb.SumCustomerLedgerBeforeParams{
		CustomerID: pgCustomer,
		CreatedAt:  pgTimestamp(from),
	})
	if err != nil {
		return StatementDTO{}, err
	}

	lines, err := s.queries.SumCustomerLedgerByTypeInWindow(ctx, billingdb.SumCustomerLedgerByTypeInWindowParams{
		CustomerID:  pgCustomer,
		CreatedAt:   pgTimestamp(from),
		CreatedAt_2: pgTimestamp(to),
	})
	if err != nil {
		return StatementDTO{}, err
	}

	var periodSum int64
	outLines := make([]billing.InvoiceLineDTO, 0, len(lines))
	for _, line := range lines {
		periodSum += line.AmountMicro
		outLines = append(outLines, billing.InvoiceLineDTO{
			LedgerType:  line.LedgerType,
			AmountMicro: line.AmountMicro,
			EntryCount:  line.EntryCount,
		})
	}
	closing := opening + periodSum

	invoices, err := s.queries.ListCustomerInvoicesInWindow(ctx, billingdb.ListCustomerInvoicesInWindowParams{
		CustomerID: pgCustomer,
		Column2:    pgDate(from),
		Column3:    pgDate(to),
	})
	if err != nil {
		return StatementDTO{}, err
	}

	invoiceDTOs := make([]InvoiceSummaryDTO, 0, len(invoices))
	var invoiceTotal int64
	for _, inv := range invoices {
		month := ""
		if inv.BillingMonth.Valid {
			month = inv.BillingMonth.Time.UTC().Format("2006-01")
		}
		invoiceDTOs = append(invoiceDTOs, InvoiceSummaryDTO{
			ID:            uuidString(inv.ID),
			BillingMonth:  month,
			SubtotalMicro: inv.SubtotalMicro,
			TaxMicro:      inv.TaxMicro,
			TotalMicro:    inv.TotalMicro,
			Status:        string(inv.Status),
			Currency:      inv.Currency,
		})
		if inv.Status == billingdb.BillingInvoiceStatusFINALIZED {
			invoiceTotal += inv.TotalMicro
		}
	}

	payments, err := s.queries.ListCustomerPaymentTopupsInWindow(ctx, billingdb.ListCustomerPaymentTopupsInWindowParams{
		CustomerID:  pgCustomer,
		CreatedAt:   pgTimestamp(from),
		CreatedAt_2: pgTimestamp(to),
		Limit:       100,
	})
	if err != nil {
		return StatementDTO{}, err
	}
	paymentDTOs := make([]PaymentSummaryDTO, 0, len(payments))
	for _, p := range payments {
		intentID := ""
		if p.PaymentIntentID.Valid {
			intentID = uuid.UUID(p.PaymentIntentID.Bytes).String()
		}
		createdAt := ""
		if p.CreatedAt.Valid {
			createdAt = p.CreatedAt.Time.UTC().Format(time.RFC3339)
		}
		paymentDTOs = append(paymentDTOs, PaymentSummaryDTO{
			LedgerID:        p.ID,
			AmountMicro:     p.Amount,
			PaymentIntentID: intentID,
			CreatedAt:       createdAt,
		})
	}

	cust, err := s.queries.GetCustomerBalance(ctx, pgCustomer)
	if err != nil {
		return StatementDTO{}, err
	}
	profile := billing.ProfileFromDB(billingdb.BillingCustomerTaxProfile{})
	if row, perr := s.queries.GetCustomerTaxProfile(ctx, pgCustomer); perr == nil {
		profile = billing.ProfileFromDB(row)
	} else if !errors.Is(perr, pgx.ErrNoRows) {
		return StatementDTO{}, perr
	}
	spendMicro, err := s.queries.SumCustomerSpendInWindow(ctx, billingdb.SumCustomerSpendInWindowParams{
		CustomerID:  pgCustomer,
		CreatedAt:   pgTimestamp(from),
		CreatedAt_2: pgTimestamp(to),
	})
	if err != nil {
		return StatementDTO{}, err
	}
	taxMicro, rateBPS := billing.NewTaxCalculator().Compute(spendMicro, profile)

	return StatementDTO{
		CustomerID:          customerID.String(),
		Period:              PeriodBounds{From: from, To: to},
		OpeningBalanceMicro: opening,
		ClosingBalanceMicro: closing,
		Lines:               outLines,
		Invoices:            invoiceDTOs,
		Payments:            paymentDTOs,
		TaxBreakdown: TaxBreakdownDTO{
			Scheme:   string(profile.Scheme),
			RateBps:  rateBPS,
			TaxMicro: taxMicro,
		},
		Reconciliation: ReconciliationDTO{
			InvoiceTotalMicro: invoiceTotal,
			LedgerSumMicro:    periodSum,
			DeltaMicro:        invoiceTotal - periodSum,
		},
		Currency: cust.Currency,
	}, nil
}

// GetWallet returns the wallet card for one customer.
func (s *CompositeReadService) GetWallet(ctx context.Context, customerID uuid.UUID) (WalletDTO, error) {
	if s == nil || s.pool == nil {
		return WalletDTO{}, fmt.Errorf("composite read service not configured")
	}
	pgCustomer := pgtype.UUID{Bytes: customerID, Valid: true}
	row, err := s.queries.GetCustomerWalletRow(ctx, pgCustomer)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WalletDTO{}, billing.ErrCustomerNotFound
		}
		return WalletDTO{}, err
	}

	wallet := WalletDTO{
		CustomerID:            customerID.String(),
		BalanceMicro:          row.Balance,
		Currency:              row.Currency,
		AllowedOverdraftMicro: row.AllowedOverdraft,
		PaymentProvider:       "placeholder",
	}
	if s.provider != nil {
		wallet.PaymentProvider = s.provider.Name()
		wallet.PaymentProviderConfigured = s.provider.Configured()
	}
	if s.cfg != nil {
		wallet.LowBalanceThresholdMicro = s.cfg.Management.LowBalanceThresholdMicro
	}

	lastAt, err := s.queries.GetCustomerLastInvoiceAt(ctx, pgCustomer)
	if err == nil && lastAt.Valid && lastAt.Time.Year() > 1970 {
		wallet.LastInvoiceAt = lastAt.Time.UTC().Format(time.RFC3339)
	}

	spend7d, err := s.queries.SumCustomerSpendLast7Days(ctx, pgCustomer)
	if err == nil && spend7d > 0 && row.Balance > 0 {
		daily := spend7d / 7
		if daily > 0 {
			days := int(row.Balance / daily)
			wallet.BurnDaysEstimate = &days
		}
	}
	return wallet, nil
}

// ListLedgerLines returns paginated ledger rows for an invoice billing month.
func (s *CompositeReadService) ListLedgerLines(ctx context.Context, customerID uuid.UUID, month time.Time, cursorID int64, limit int32) ([]LedgerLineDTO, string, int64, error) {
	if s == nil || s.pool == nil {
		return nil, "", 0, fmt.Errorf("composite read service not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}
	monthStart := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, 0)

	pgCustomer := pgtype.UUID{Bytes: customerID, Valid: true}
	total, err := s.queries.CountCustomerLedgerInWindow(ctx, billingdb.CountCustomerLedgerInWindowParams{
		CustomerID:  pgCustomer,
		CreatedAt:   pgTimestamp(monthStart),
		CreatedAt_2: pgTimestamp(monthEnd),
	})
	if err != nil {
		return nil, "", 0, err
	}

	rows, err := s.queries.ListCustomerLedgerInWindow(ctx, billingdb.ListCustomerLedgerInWindowParams{
		CustomerID:  pgCustomer,
		CreatedAt:   pgTimestamp(monthStart),
		CreatedAt_2: pgTimestamp(monthEnd),
		Column4:     cursorID,
		Limit:       limit,
	})
	if err != nil {
		return nil, "", 0, err
	}

	out := make([]LedgerLineDTO, 0, len(rows))
	var lastID int64
	for _, row := range rows {
		lastID = row.ID
		createdAt := ""
		if row.CreatedAt.Valid {
			createdAt = row.CreatedAt.Time.UTC().Format(time.RFC3339)
		}
		out = append(out, LedgerLineDTO{
			ID:          row.ID,
			AmountMicro: row.Amount,
			LedgerType:  string(row.Type),
			CreatedAt:   createdAt,
		})
	}
	nextCursor := ""
	if int32(len(out)) == limit && lastID > 0 {
		nextCursor = fmt.Sprintf("%d", lastID)
	}
	return out, nextCursor, total, nil
}

// ListLedgerLinesInWindow returns paginated ledger rows for an arbitrary UTC window.
func (s *CompositeReadService) ListLedgerLinesInWindow(ctx context.Context, customerID uuid.UUID, from, to time.Time, cursorID int64, limit int32) ([]LedgerLineDTO, string, error) {
	if s == nil || s.pool == nil {
		return nil, "", fmt.Errorf("composite read service not configured")
	}
	if limit <= 0 {
		limit = 1000
	}
	pgCustomer := pgtype.UUID{Bytes: customerID, Valid: true}
	rows, err := s.queries.ListCustomerLedgerInWindow(ctx, billingdb.ListCustomerLedgerInWindowParams{
		CustomerID:  pgCustomer,
		CreatedAt:   pgTimestamp(from),
		CreatedAt_2: pgTimestamp(to),
		Column4:     cursorID,
		Limit:       limit,
	})
	if err != nil {
		return nil, "", err
	}
	out := make([]LedgerLineDTO, 0, len(rows))
	var lastID int64
	for _, row := range rows {
		lastID = row.ID
		createdAt := ""
		if row.CreatedAt.Valid {
			createdAt = row.CreatedAt.Time.UTC().Format(time.RFC3339)
		}
		out = append(out, LedgerLineDTO{
			ID:          row.ID,
			AmountMicro: row.Amount,
			LedgerType:  string(row.Type),
			CreatedAt:   createdAt,
		})
	}
	next := ""
	if int32(len(out)) == limit && lastID > 0 {
		next = fmt.Sprintf("%d", lastID)
	}
	return out, next, nil
}

// GetInvariant returns per-customer or global ledger invariant status.
func (s *CompositeReadService) GetInvariant(ctx context.Context, customerID *uuid.UUID) (InvariantDTO, error) {
	if s == nil || s.pool == nil {
		return InvariantDTO{}, fmt.Errorf("composite read service not configured")
	}
	if customerID != nil {
		snap, err := billing.ReadLedgerInvariant(ctx, s.pool, *customerID)
		if err != nil {
			return InvariantDTO{}, err
		}
		diff := snap.BalanceMicro - snap.LedgerSumMicro
		ok := diff >= -ledgerInvariantToleranceMicro && diff <= ledgerInvariantToleranceMicro
		return InvariantDTO{
			OK:             ok,
			CustomerID:     customerID.String(),
			BalanceMicro:   snap.BalanceMicro,
			LedgerSumMicro: snap.LedgerSumMicro,
			DiffMicro:      diff,
		}, nil
	}

	rows, err := s.pool.Query(ctx, `SELECT id FROM customers ORDER BY id LIMIT 500`)
	if err != nil {
		return InvariantDTO{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var id pgtype.UUID
		if err := rows.Scan(&id); err != nil {
			return InvariantDTO{}, err
		}
		cid := uuid.UUID(id.Bytes)
		snap, err := billing.ReadLedgerInvariant(ctx, s.pool, cid)
		if err != nil {
			return InvariantDTO{}, err
		}
		diff := snap.BalanceMicro - snap.LedgerSumMicro
		if diff < -ledgerInvariantToleranceMicro || diff > ledgerInvariantToleranceMicro {
			return InvariantDTO{
				OK:             false,
				CustomerID:     cid.String(),
				BalanceMicro:   snap.BalanceMicro,
				LedgerSumMicro: snap.LedgerSumMicro,
				DiffMicro:      diff,
			}, nil
		}
	}
	return InvariantDTO{OK: true}, rows.Err()
}

// GetSummary returns ops billing dashboard aggregates.
func (s *CompositeReadService) GetSummary(ctx context.Context) (SummaryDTO, error) {
	if s == nil || s.pool == nil {
		return SummaryDTO{}, fmt.Errorf("composite read service not configured")
	}
	mtd, err := s.queries.SumInvoicesMTD(ctx)
	if err != nil {
		return SummaryDTO{}, err
	}

	monthStart := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, 0)
	customersSpend, err := s.queries.CountCustomersWithFeeSpendInWindow(ctx, billingdb.CountCustomersWithFeeSpendInWindowParams{
		CreatedAt:   pgTimestamp(monthStart),
		CreatedAt_2: pgTimestamp(monthEnd),
	})
	if err != nil {
		return SummaryDTO{}, err
	}

	var undelivered int64
	err = s.pool.QueryRow(ctx, `
		SELECT COUNT(*)::bigint
		FROM notifier.notifications
		WHERE template_id = 'invoice_monthly' AND status NOT IN ('SENT')`).Scan(&undelivered)
	if err != nil {
		return SummaryDTO{}, err
	}

	return SummaryDTO{
		InvoicedMTDMicro:                mtd.Column1,
		InvoiceCountMTD:                 mtd.Column2,
		UndeliveredInvoiceNotifications: undelivered,
		CustomersWithSpendInMonth:       customersSpend,
	}, nil
}

// ListDeliveries returns notifier rows for an invoice dedup key.
func (s *CompositeReadService) ListDeliveries(ctx context.Context, invoiceID string) ([]DeliveryDTO, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("composite read service not configured")
	}
	dedupKey := "invoice:" + invoiceID
	rows, err := s.pool.Query(ctx, `
		SELECT id, status::text, provider::text, recipient, template_id, error_message, retry_count, created_at, updated_at
		FROM notifier.notifications
		WHERE dedup_key = $1
		ORDER BY created_at DESC`, dedupKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]DeliveryDTO, 0, 4)
	for rows.Next() {
		var (
			id, status, provider, recipient, templateID string
			errorMessage                                pgtype.Text
			retryCount                                  int32
			createdAt, updatedAt                        time.Time
		)
		if err := rows.Scan(&id, &status, &provider, &recipient, &templateID, &errorMessage, &retryCount, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		dto := DeliveryDTO{
			ID:         id,
			Status:     status,
			Provider:   provider,
			Recipient:  recipient,
			TemplateID: templateID,
			RetryCount: retryCount,
			CreatedAt:  createdAt.UTC().Format(time.RFC3339),
			UpdatedAt:  updatedAt.UTC().Format(time.RFC3339),
		}
		if errorMessage.Valid {
			dto.ErrorMessage = errorMessage.String
		}
		out = append(out, dto)
	}
	return out, rows.Err()
}

// GetTaxProfile returns stored customer tax metadata.
func (s *CompositeReadService) GetTaxProfile(ctx context.Context, customerID uuid.UUID) (TaxProfileDTO, error) {
	if s == nil || s.queries == nil {
		return TaxProfileDTO{}, fmt.Errorf("composite read service not configured")
	}
	row, err := s.queries.GetCustomerTaxProfile(ctx, pgtype.UUID{Bytes: customerID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			calc := billing.NewTaxCalculator()
			def := calc.DefaultProfile("", "")
			return TaxProfileDTO{
				CustomerID:  customerID.String(),
				CountryCode: def.CountryCode,
				TaxScheme:   string(def.Scheme),
				TaxRateBps:  def.RateBPS,
			}, nil
		}
		return TaxProfileDTO{}, err
	}
	dto := TaxProfileDTO{
		CustomerID:  customerID.String(),
		CountryCode: row.CountryCode,
		TaxScheme:   string(row.TaxScheme),
		TaxRateBps:  row.TaxRateBps,
	}
	if row.TaxRegion.Valid {
		dto.TaxRegion = row.TaxRegion.String
	}
	return dto, nil
}

// UpsertTaxProfile stores customer tax metadata.
func (s *CompositeReadService) UpsertTaxProfile(ctx context.Context, customerID uuid.UUID, dto TaxProfileDTO) (TaxProfileDTO, error) {
	if s == nil || s.queries == nil {
		return TaxProfileDTO{}, fmt.Errorf("composite read service not configured")
	}
	row, err := s.queries.UpsertCustomerTaxProfile(ctx, billingdb.UpsertCustomerTaxProfileParams{
		CustomerID:  pgtype.UUID{Bytes: customerID, Valid: true},
		CountryCode: dto.CountryCode,
		TaxRegion:   pgtype.Text{String: dto.TaxRegion, Valid: dto.TaxRegion != ""},
		TaxScheme:   billingdb.BillingTaxScheme(dto.TaxScheme),
		TaxRateBps:  dto.TaxRateBps,
	})
	if err != nil {
		return TaxProfileDTO{}, err
	}
	out := TaxProfileDTO{
		CustomerID:  customerID.String(),
		CountryCode: row.CountryCode,
		TaxScheme:   string(row.TaxScheme),
		TaxRateBps:  row.TaxRateBps,
	}
	if row.TaxRegion.Valid {
		out.TaxRegion = row.TaxRegion.String
	}
	return out, nil
}

// ParseStatementPeriod resolves from/to query params (RFC3339 or billing month).
func ParseStatementPeriod(fromRaw, toRaw, monthRaw string) (time.Time, time.Time, error) {
	if monthRaw != "" {
		month, err := time.Parse("2006-01", monthRaw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid month")
		}
		start := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, 0), nil
	}
	if fromRaw == "" || toRaw == "" {
		now := time.Now().UTC()
		start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, 0), nil
	}
	from, err := time.Parse(time.RFC3339, fromRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid from")
	}
	to, err := time.Parse(time.RFC3339, toRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid to")
	}
	return from.UTC(), to.UTC(), nil
}

func pgTimestamp(t time.Time) pgtype.Timestamp {
	return pgtype.Timestamp{Time: t.UTC(), Valid: true}
}

func pgDate(t time.Time) pgtype.Date {
	return pgtype.Date{Time: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), Valid: true}
}

func uuidString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	return uuid.UUID(id.Bytes).String()
}
