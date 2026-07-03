package management

import (
	"context"
	"fmt"

	"espx/internal/config"
	paymentpb "espx/internal/payment/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// PaymentClient calls the payment gRPC service from management after RBAC checks.
type PaymentClient struct {
	conn   *grpc.ClientConn
	client paymentpb.PaymentServiceClient
	token  string
}

// NewPaymentClient dials payment only when PAYMENT_INTERNAL_TOKEN is set so management boots without payment in minimal stacks.
func NewPaymentClient(cfg *config.Config) (*PaymentClient, error) {
	if cfg == nil || string(cfg.PaymentInternalToken) == "" {
		return nil, nil
	}

	host := cfg.PaymentServerHost
	if host == "" {
		host = "127.0.0.1"
	}
	target := host + ":" + cfg.PaymentServerPort

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("payment gRPC dial %s: %w", target, err)
	}

	return &PaymentClient{
		conn:   conn,
		client: paymentpb.NewPaymentServiceClient(conn),
		token:  string(cfg.PaymentInternalToken),
	}, nil
}

// Close releases the gRPC connection on management shutdown to avoid leaked conns during deploy restarts.
func (c *PaymentClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// CreatePaymentIntent attaches the internal token because payment gRPC rejects unauthenticated callers.
func (c *PaymentClient) CreatePaymentIntent(ctx context.Context, customerID string, amountMicro int64, currency, idempotencyKey string, meta map[string]string) (*paymentpb.CreatePaymentIntentResponse, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("payment client not configured")
	}
	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", c.token)
	return c.client.CreatePaymentIntent(grpcCtx, &paymentpb.CreatePaymentIntentRequest{
		CustomerId:     customerID,
		AmountMicro:    amountMicro,
		Currency:       currency,
		IdempotencyKey: idempotencyKey,
		Metadata:       meta,
	})
}
