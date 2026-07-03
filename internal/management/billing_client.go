package management

import (
	"context"
	"fmt"
	"time"

	"espx/internal/billing/pb"
	"espx/internal/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BillingClient calls the billing gRPC service from management after RBAC checks.
type BillingClient struct {
	conn   *grpc.ClientConn
	client pb.BillingServiceClient
	token  string
}

// NewBillingClient dials billing only when BILLING_INTERNAL_TOKEN is set.
func NewBillingClient(cfg *config.Config) (*BillingClient, error) {
	if cfg == nil || string(cfg.BillingInternalToken) == "" {
		return nil, nil
	}

	host := cfg.Billing.ServerHost
	if host == "" {
		host = "127.0.0.1"
	}
	target := host + ":" + cfg.Billing.Port

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("billing gRPC dial %s: %w", target, err)
	}

	return &BillingClient{
		conn:   conn,
		client: pb.NewBillingServiceClient(conn),
		token:  string(cfg.BillingInternalToken),
	}, nil
}

// Close releases the gRPC connection on management shutdown.
func (client *BillingClient) Close() error {
	if client == nil || client.conn == nil {
		return nil
	}
	return client.conn.Close()
}

// GenerateInvoice proxies invoice generation to the billing service.
func (client *BillingClient) GenerateInvoice(ctx context.Context, customerID string, billingMonth time.Time) (*pb.Invoice, error) {
	if client == nil || client.client == nil {
		return nil, fmt.Errorf("billing client not configured")
	}
	month := time.Date(billingMonth.Year(), billingMonth.Month(), 1, 0, 0, 0, 0, time.UTC)
	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", client.token)
	return client.client.GenerateInvoice(grpcCtx, &pb.GenerateInvoiceRequest{
		CustomerId:   customerID,
		BillingMonth: timestamppb.New(month),
	})
}

// ListInvoices returns paginated invoice history for the HTMX dashboard.
func (client *BillingClient) ListInvoices(ctx context.Context, customerID string, limit, offset int32) (*pb.ListInvoicesResponse, error) {
	if client == nil || client.client == nil {
		return nil, fmt.Errorf("billing client not configured")
	}
	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", client.token)
	return client.client.ListInvoices(grpcCtx, &pb.ListInvoicesRequest{
		CustomerId: customerID,
		Limit:      limit,
		Offset:     offset,
	})
}
