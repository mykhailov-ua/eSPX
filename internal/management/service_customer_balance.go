package management

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"

	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
)

const (
	ledgerExportMaxBytes   = 10 * 1024 * 1024
	ledgerExportBatchLimit = 500
)

// CustomerBalanceDTO is the JSON payload for GET /api/v1/customers/{id}/balance.
type CustomerBalanceDTO struct {
	CustomerID string      `json:"customer_id"`
	Balance    string      `json:"balance"`
	Currency   string      `json:"currency"`
	Ledger     []LedgerDTO `json:"ledger"`
}

// LedgerExportResult captures cursor continuation metadata after a capped CSV stream.
type LedgerExportResult struct {
	NextCursor int64
	Truncated  bool
	Bytes      int
}

// GetCustomerBalance returns balance and the 100 most recent ledger rows by id.
func (s *Service) GetCustomerBalance(ctx context.Context, customerID uuid.UUID) (CustomerBalanceDTO, error) {
	q := db.New(s.GetPool())
	cust, err := q.GetCustomerByID(ctx, ingestion.ToUUID(customerID))
	if err != nil {
		return CustomerBalanceDTO{}, mapNotFound(err, ErrCustomerNotFound)
	}

	rows, err := q.ListCustomerLedgerByIDDesc(ctx, ingestion.ToUUID(customerID))
	if err != nil {
		return CustomerBalanceDTO{}, err
	}

	ledger := make([]LedgerDTO, 0, len(rows))
	for _, row := range rows {
		ledger = append(ledger, ledgerToDTO(row))
	}

	return CustomerBalanceDTO{
		CustomerID: customerID.String(),
		Balance:    formatMicro(cust.Balance),
		Currency:   cust.Currency,
		Ledger:     ledger,
	}, nil
}

// ExportCustomerLedgerCSV streams ledger rows as CSV up to ledgerExportMaxBytes.
func (s *Service) ExportCustomerLedgerCSV(ctx context.Context, customerID uuid.UUID, cursor int64, w io.Writer) (LedgerExportResult, error) {
	q := db.New(s.GetPool())
	if _, err := q.GetCustomerByID(ctx, ingestion.ToUUID(customerID)); err != nil {
		return LedgerExportResult{}, err
	}

	limited := &limitedWriter{w: w, limit: ledgerExportMaxBytes}
	cw := csv.NewWriter(limited)
	if err := cw.Write([]string{"id", "customer_id", "campaign_id", "amount", "type", "idempotency_hash", "created_at"}); err != nil {
		return LedgerExportResult{}, err
	}

	var (
		nextCursor = cursor
		truncated  bool
		lastID     int64
	)

	for {
		rows, err := q.ListCustomerLedgerExport(ctx, db.ListCustomerLedgerExportParams{
			CustomerID: ingestion.ToUUID(customerID),
			CursorID:   nextCursor,
			BatchLimit: ledgerExportBatchLimit,
		})
		if err != nil {
			return LedgerExportResult{}, err
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			if limited.remaining() <= 0 {
				truncated = true
				goto done
			}
			campID := ""
			if row.CampaignID.Valid {
				campID = uuid.UUID(row.CampaignID.Bytes).String()
			}
			record := []string{
				strconv.FormatInt(row.ID, 10),
				uuid.UUID(row.CustomerID.Bytes).String(),
				campID,
				formatMicro(row.Amount),
				string(row.Type),
				row.IdempotencyHash.String,
				row.CreatedAt.Time.UTC().Format(time.RFC3339),
			}
			if err := cw.Write(record); err != nil {
				if limited.overflow() {
					truncated = true
					goto done
				}
				return LedgerExportResult{}, err
			}
			lastID = row.ID
		}

		if len(rows) < ledgerExportBatchLimit {
			break
		}
		nextCursor = lastID
		if limited.remaining() <= 0 {
			truncated = true
			break
		}
	}

done:
	cw.Flush()
	if err := cw.Error(); err != nil {
		if limited.overflow() {
			truncated = true
		} else {
			return LedgerExportResult{}, err
		}
	}

	result := LedgerExportResult{
		Truncated: truncated,
		Bytes:     limited.bytesWritten(),
	}
	if truncated && lastID > 0 {
		result.NextCursor = lastID
	}
	return result, nil
}

type limitedWriter struct {
	w          io.Writer
	limit      int
	n          int
	overflowed bool
}

func (lw *limitedWriter) bytesWritten() int { return lw.n }

func (lw *limitedWriter) Write(p []byte) (int, error) {
	if lw.remaining() <= 0 {
		lw.overflowed = true
		return 0, errExportLimit
	}
	if len(p) > lw.remaining() {
		p = p[:lw.remaining()]
	}
	n, err := lw.w.Write(p)
	lw.n += n
	if n < len(p) || (err == nil && lw.remaining() == 0 && len(p) > 0) {
		lw.overflowed = true
	}
	if err != nil {
		return n, err
	}
	if lw.overflowed {
		return n, errExportLimit
	}
	return n, nil
}

func (lw *limitedWriter) remaining() int {
	return lw.limit - lw.n
}

func (lw *limitedWriter) overflow() bool {
	return lw.overflowed
}

var errExportLimit = fmt.Errorf("export byte limit reached")
