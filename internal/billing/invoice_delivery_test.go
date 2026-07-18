package billing

import (
	"context"
	"testing"
	"time"

	billingpb "espx/internal/billing/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestService_invoicePDFURL_usesAdminAPIPath(t *testing.T) {
	t.Parallel()
	svc := &Service{invoiceBaseURL: "https://admin.example.com"}
	assert.Equal(t,
		"https://admin.example.com/api/v1/billing/invoices/inv-1/pdf",
		svc.invoicePDFURL("inv-1"),
	)
}

func TestRenderInvoicePDF_nonEmpty(t *testing.T) {
	t.Parallel()
	month := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	pdf := RenderInvoicePDF(&billingpb.Invoice{
		Id:            "inv-1",
		CustomerId:    "cust-1",
		BillingMonth:  timestamppb.New(month),
		SubtotalMicro: 2_500_000,
		TaxMicro:      250_000,
		TotalMicro:    2_750_000,
		Currency:      "USD",
	})
	require.NotEmpty(t, pdf)
	assert.Contains(t, string(pdf), "%PDF-1.4")
}

func TestService_GenerateInvoice_skipsEmptySpend(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanup := setupBillingTestDB(t)
	defer cleanup()

	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	customerID := seedCustomerOnly(t, pool)
	svc := NewService(pool)

	_, err := svc.GenerateInvoice(ctx, customerID, month)
	require.ErrorIs(t, err, ErrNoSpend)
}
