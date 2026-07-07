package management

import (
	"context"
	"testing"

	"espx/internal/ads"
	adsdb "espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/management/pb"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestSettlementHandler_GetLedgerEntry(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{SettlementInternalToken: "settlement-test-token"}
	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, cfg)
	defer svc.Close()

	handler := NewSettlementHandler(svc, cfg)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-internal-token", "settlement-test-token"))

	missingIntentID := uuid.New()
	missingResp, err := handler.GetLedgerEntry(ctx, &pb.GetLedgerEntryRequest{
		PaymentIntentId: missingIntentID.String(),
	})
	require.NoError(t, err)
	assert.False(t, missingResp.Found)

	customerID := uuid.New()
	intentID := uuid.New()
	q := adsdb.New(pool)
	_, err = q.CreateCustomer(context.Background(), adsdb.CreateCustomerParams{
		ID:       ads.ToUUID(customerID),
		Name:     "ledger lookup customer",
		Balance:  0,
		Currency: "USD",
	})
	require.NoError(t, err)

	applied, ledgerID, err := svc.ApplyPaymentCredit(
		context.Background(),
		customerID,
		12_500_000,
		"payment:"+intentID.String(),
		intentID,
		"stripe",
		"pi_test_ledger_lookup",
	)
	require.NoError(t, err)
	require.True(t, applied)
	require.NotZero(t, ledgerID)

	foundResp, err := handler.GetLedgerEntry(ctx, &pb.GetLedgerEntryRequest{
		PaymentIntentId: intentID.String(),
	})
	require.NoError(t, err)
	require.True(t, foundResp.Found)
	require.NotNil(t, foundResp.Topup)
	assert.Equal(t, ledgerID, foundResp.Topup.Id)
	assert.Equal(t, customerID.String(), foundResp.Topup.CustomerId)
	assert.Equal(t, int64(12_500_000), foundResp.Topup.AmountMicro)
	assert.Equal(t, "PAYMENT_TOPUP", foundResp.Topup.Type)
	assert.Zero(t, foundResp.RefundTotalMicro)
}
