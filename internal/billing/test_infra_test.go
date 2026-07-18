package billing

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"espx/internal/ingestion"
	ingestdb "espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
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
	applyBillingMigrations(t, pool, filepath.Join(baseDir, "internal/ingestion/migrations"))
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
	q := ingestdb.New(pool)

	customerID, _ := uuid.NewV7()
	_, err := q.CreateCustomer(ctx, ingestdb.CreateCustomerParams{
		ID:       ingestion.ToUUID(customerID),
		Name:     "billing-test",
		Balance:  0,
		Currency: "USD",
	})
	require.NoError(t, err)

	_, err = q.CreateLedgerEntry(ctx, ingestdb.CreateLedgerEntryParams{
		CustomerID:      ingestion.ToUUID(customerID),
		Amount:          10_000_000,
		Type:            ingestdb.LedgerTypeTOPUP,
		IdempotencyHash: pgtype.Text{String: "topup-" + customerID.String(), Valid: true},
	})
	require.NoError(t, err)
	_, err = q.UpdateCustomerBalanceManagement(ctx, ingestdb.UpdateCustomerBalanceManagementParams{
		ID:      ingestion.ToUUID(customerID),
		Balance: 10_000_000,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO balance_ledger (customer_id, amount, type, created_at)
		VALUES ($1, $2, 'FEE', $3)
	`, customerID, -2_500_000, feeAt)
	require.NoError(t, err)
	_, err = q.UpdateCustomerBalanceManagement(ctx, ingestdb.UpdateCustomerBalanceManagementParams{
		ID:      ingestion.ToUUID(customerID),
		Balance: -2_500_000,
	})
	require.NoError(t, err)

	AssertLedgerBalanceInvariant(t, ctx, pool, customerID)
	return customerID
}

func seedCustomerOnly(t testing.TB, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	q := ingestdb.New(pool)
	customerID, _ := uuid.NewV7()
	_, err := q.CreateCustomer(ctx, ingestdb.CreateCustomerParams{
		ID:       ingestion.ToUUID(customerID),
		Name:     "billing-empty",
		Balance:  0,
		Currency: "USD",
	})
	require.NoError(t, err)
	return customerID
}
