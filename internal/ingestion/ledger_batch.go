package ingestion

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrInsufficientCustomerBalance is returned when a consolidated spend batch exceeds customer funds.
var ErrInsufficientCustomerBalance = errors.New("insufficient customer balance for spend batch")

const (
	defaultLedgerBatchFlush = 10 * time.Second
	maxLedgerBatchSize      = 32
)

// SpendFlushItem is one campaign's consolidated Redis sync window awaiting PG commit.
type SpendFlushItem struct {
	CampaignID  uuid.UUID
	AmountMicro int64
	TxID        string
}

// SpendFlushOutcome records per-campaign batch flush result.
type SpendFlushOutcome struct {
	CampaignID uuid.UUID
	Err        error // nil = applied or duplicate; ErrInsufficientCustomerBalance pauses delivery
}

// spendBatchFlusher batches consolidated campaign spend into one Postgres transaction.
type spendBatchFlusher interface {
	UpdateSpendBatch(ctx context.Context, items []SpendFlushItem) ([]SpendFlushOutcome, error)
}

// pendingRollup holds one campaign's inflight Redis sync awaiting a consolidated PG flush (M12).
type pendingRollup struct {
	amountMicro int64
	txID        string
	idStr       string
	syncKey     string
	inFlightKey string
	lockKey     string
	txKey       string
	dirtySet    string
}

func ledgerBatchHash(txID string) string {
	return "spend_batch:" + txID
}
