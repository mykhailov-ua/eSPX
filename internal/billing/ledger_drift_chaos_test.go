package billing

import (
	"context"
	"espx/internal/ads/repo"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	adsdb "espx/internal/ads/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupBillingTestDB(t testing.TB) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("billing_test_db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("secure_password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(20*time.Second)),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	_, filename, _, _ := runtime.Caller(0)
	baseDir := filepath.Join(filepath.Dir(filename), "..", "..")
	applyBillingMigrations(t, pool, filepath.Join(baseDir, "internal/ads/migrations"))
	applyBillingMigrations(t, pool, filepath.Join(baseDir, "internal/billing/migrations"))

	return pool, func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
}

func applyBillingMigrations(t testing.TB, pool *pgxpool.Pool, dir string) {
	t.Helper()
	ctx := context.Background()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)

		sql := string(sqlBytes)
		parts := strings.Split(sql, "-- +goose Down")
		upPart := parts[0]
		upPart = strings.ReplaceAll(upPart, "-- +goose Up", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementBegin", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementEnd", "")
		if strings.TrimSpace(upPart) == "" {
			continue
		}
		_, err = pool.Exec(ctx, upPart)
		require.NoError(t, err, "migration %s", entry.Name())
	}
}

func seedCustomerWithLedger(t testing.TB, pool *pgxpool.Pool, feeAt time.Time) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	q := adsdb.New(pool)

	customerID, _ := uuid.NewV7()
	_, err := q.CreateCustomer(ctx, adsdb.CreateCustomerParams{
		ID:       repo.ToUUID(customerID),
		Name:     "billing-test",
		Balance:  0,
		Currency: "USD",
	})
	require.NoError(t, err)

	_, err = q.CreateLedgerEntry(ctx, adsdb.CreateLedgerEntryParams{
		CustomerID:      repo.ToUUID(customerID),
		Amount:          10_000_000,
		Type:            adsdb.LedgerTypeTOPUP,
		IdempotencyHash: pgtype.Text{String: "topup-" + customerID.String(), Valid: true},
	})
	require.NoError(t, err)
	_, err = q.UpdateCustomerBalanceManagement(ctx, adsdb.UpdateCustomerBalanceManagementParams{
		ID:      repo.ToUUID(customerID),
		Balance: 10_000_000,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO balance_ledger (customer_id, amount, type, created_at)
		VALUES ($1, $2, 'FEE', $3)
	`, customerID, -2_500_000, feeAt)
	require.NoError(t, err)
	_, err = q.UpdateCustomerBalanceManagement(ctx, adsdb.UpdateCustomerBalanceManagementParams{
		ID:      repo.ToUUID(customerID),
		Balance: -2_500_000,
	})
	require.NoError(t, err)

	AssertLedgerBalanceInvariant(t, ctx, pool, customerID)
	return customerID
}

// Guards invoice generation aggregates ledger spend and applies sales tax for US customers.
func TestService_GenerateInvoice(t *testing.T) {
	pool, cleanup := setupBillingTestDB(t)
	defer cleanup()

	ctx := context.Background()
	month := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	feeAt := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	customerID := seedCustomerWithLedger(t, pool, feeAt)
	svc := NewService(pool)

	invoice, err := svc.GenerateInvoice(ctx, customerID, month)
	require.NoError(t, err)
	require.NotNil(t, invoice)
	assert.Equal(t, customerID.String(), invoice.CustomerId)
	assert.Equal(t, int64(2_500_000), invoice.SubtotalMicro)
	assert.Greater(t, invoice.TaxMicro, int64(0))
	assert.Equal(t, invoice.SubtotalMicro+invoice.TaxMicro, invoice.TotalMicro)

	again, err := svc.GenerateInvoice(ctx, customerID, month)
	require.NoError(t, err)
	assert.Equal(t, invoice.Id, again.Id)
}

// Guards concurrent invoice generation is idempotent under parallel callers.
func TestService_GenerateInvoiceConcurrent(t *testing.T) {
	pool, cleanup := setupBillingTestDB(t)
	defer cleanup()

	ctx := context.Background()
	month := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	feeAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	customerID := seedCustomerWithLedger(t, pool, feeAt)
	svc := NewService(pool)

	const goroutines = 20
	var wg sync.WaitGroup
	var success atomic.Int32
	ids := make(chan string, goroutines)

	wg.Add(goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			inv, err := svc.GenerateInvoice(ctx, customerID, month)
			if err == nil && inv != nil {
				success.Add(1)
				ids <- inv.Id
			}
		}()
	}
	close(start)
	wg.Wait()
	close(ids)

	assert.Equal(t, int32(goroutines), success.Load())
	first := ""
	for id := range ids {
		if first == "" {
			first = id
			continue
		}
		assert.Equal(t, first, id)
	}
}

// Guards GenerateInvoice rejects ledger drift before creating an invoice.
func TestChaos_LedgerDriftCheck(t *testing.T) {
	pool, cleanup := setupBillingTestDB(t)
	defer cleanup()

	ctx := context.Background()
	customerID := seedCustomerWithLedger(t, pool, time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC))

	_, err := pool.Exec(ctx, `UPDATE customers SET balance = balance + 100 WHERE id = $1`, customerID)
	require.NoError(t, err)

	svc := NewService(pool)
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	_, err = svc.GenerateInvoice(ctx, customerID, month)
	require.ErrorIs(t, err, ErrLedgerDrift)

	logChaosProof(t, "ledger_drift_check", map[string]string{
		"subsystem":    "billing",
		"customer_id":  customerID.String(),
		"rejected":     "true",
		"invariant_ok": "false",
	})
}
