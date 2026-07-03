package billing

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const ledgerInvariantToleranceMicro = int64(1)

// LedgerInvariantSnapshot captures customer balance and ledger sum for drift detection.
type LedgerInvariantSnapshot struct {
	CustomerID     uuid.UUID
	BalanceMicro   int64
	LedgerSumMicro int64
}

// ReadLedgerInvariant loads customer balance and the sum of ledger rows for one customer.
func ReadLedgerInvariant(ctx context.Context, pool *pgxpool.Pool, customerID uuid.UUID) (LedgerInvariantSnapshot, error) {
	var snap LedgerInvariantSnapshot
	snap.CustomerID = customerID

	err := pool.QueryRow(ctx,
		`SELECT balance FROM customers WHERE id = $1`, customerID,
	).Scan(&snap.BalanceMicro)
	if err != nil {
		return snap, fmt.Errorf("read customer balance: %w", err)
	}

	err = pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount), 0)::bigint FROM balance_ledger WHERE customer_id = $1`, customerID,
	).Scan(&snap.LedgerSumMicro)
	if err != nil {
		return snap, fmt.Errorf("sum ledger: %w", err)
	}

	return snap, nil
}

// AssertLedgerBalanceInvariant verifies customers.balance equals SUM(balance_ledger.amount).
func AssertLedgerBalanceInvariant(t testing.TB, ctx context.Context, pool *pgxpool.Pool, customerID uuid.UUID) {
	t.Helper()

	snap, err := ReadLedgerInvariant(ctx, pool, customerID)
	if err != nil {
		t.Fatalf("read ledger invariant: %v", err)
	}

	diff := snap.BalanceMicro - snap.LedgerSumMicro
	if diff < -ledgerInvariantToleranceMicro || diff > ledgerInvariantToleranceMicro {
		t.Fatalf(
			"ledger invariant violated for customer %s: balance=%d ledger_sum=%d diff=%d tolerance<=%d",
			customerID,
			snap.BalanceMicro,
			snap.LedgerSumMicro,
			diff,
			ledgerInvariantToleranceMicro,
		)
	}
}

// CheckLedgerBalanceInvariant returns ErrLedgerDrift when balance and ledger sum diverge.
func CheckLedgerBalanceInvariant(ctx context.Context, pool *pgxpool.Pool, customerID uuid.UUID) error {
	snap, err := ReadLedgerInvariant(ctx, pool, customerID)
	if err != nil {
		return err
	}
	diff := snap.BalanceMicro - snap.LedgerSumMicro
	if diff < -ledgerInvariantToleranceMicro || diff > ledgerInvariantToleranceMicro {
		return fmt.Errorf("%w: balance=%d ledger_sum=%d diff=%d", ErrLedgerDrift, snap.BalanceMicro, snap.LedgerSumMicro, diff)
	}
	return nil
}
