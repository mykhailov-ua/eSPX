package payment

import (
	"context"
	"fmt"
	"sync"

	mgmtpb "espx/internal/management/pb"
	"espx/internal/config"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// SettlementLedgerClient reads ledger state from management settlement gRPC.
type SettlementLedgerClient struct {
	cfg    *config.Config
	mu     sync.Mutex
	conn   *grpc.ClientConn
	client mgmtpb.SettlementServiceClient
}

// NewSettlementLedgerClient constructs a lazy settlement reader for payment recon.
func NewSettlementLedgerClient(cfg *config.Config) *SettlementLedgerClient {
	return &SettlementLedgerClient{cfg: cfg}
}

// PaymentIntentLedger aggregates ledger totals returned by GetLedgerEntry.
type PaymentIntentLedger struct {
	TopupMicro              int64
	RefundMicro             int64
	ChargebackMicro         int64
	ChargebackReversalMicro int64
	HasTopup               bool
}

// GetPaymentIntentLedger loads ledger totals for one payment intent via settlement gRPC.
func (c *SettlementLedgerClient) GetPaymentIntentLedger(ctx context.Context, intentID uuid.UUID) (PaymentIntentLedger, error) {
	if err := c.ensureClient(); err != nil {
		return PaymentIntentLedger{}, err
	}
	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", string(c.cfg.SettlementInternalToken))
	resp, err := c.getClient().GetLedgerEntry(grpcCtx, &mgmtpb.GetLedgerEntryRequest{
		PaymentIntentId: intentID.String(),
	})
	if err != nil {
		return PaymentIntentLedger{}, fmt.Errorf("settlement GetLedgerEntry: %w", err)
	}
	out := PaymentIntentLedger{
		RefundMicro:             resp.GetRefundTotalMicro(),
		ChargebackMicro:         resp.GetChargebackTotalMicro(),
		ChargebackReversalMicro: resp.GetChargebackReversalTotalMicro(),
	}
	if resp.GetFound() && resp.GetTopup() != nil {
		out.HasTopup = true
		out.TopupMicro = resp.GetTopup().GetAmountMicro()
	}
	return out, nil
}

// Close releases the gRPC connection.
func (c *SettlementLedgerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.client = nil
		return err
	}
	return nil
}

func (c *SettlementLedgerClient) ensureClient() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return nil
	}
	target := c.cfg.SettlementServerHost + ":" + c.cfg.SettlementServerPort
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial settlement %s: %w", target, err)
	}
	c.conn = conn
	c.client = mgmtpb.NewSettlementServiceClient(conn)
	return nil
}

func (c *SettlementLedgerClient) getClient() mgmtpb.SettlementServiceClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client
}
