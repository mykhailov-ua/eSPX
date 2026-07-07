package management

import (
	"context"
	"crypto/subtle"
	"errors"
	"espx/internal/config"
	"espx/internal/management/pb"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type SettlementHandler struct {
	pb.UnimplementedSettlementServiceServer
	service *Service
	cfg     *config.Config
}

func NewSettlementHandler(service *Service, cfg *config.Config) *SettlementHandler {
	return &SettlementHandler{
		service: service,
		cfg:     cfg,
	}
}

func (h *SettlementHandler) ApplyPaymentCredit(ctx context.Context, req *pb.ApplyPaymentCreditRequest) (*pb.ApplyPaymentCreditResponse, error) {
	if err := h.requireSettlementToken(ctx); err != nil {
		return nil, err
	}

	customerID, err := uuid.Parse(req.CustomerId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid customer id")
	}
	paymentIntentID, err := uuid.Parse(req.PaymentIntentId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payment intent id")
	}

	applied, ledgerEntryID, err := h.service.ApplyPaymentCredit(
		ctx,
		customerID,
		req.AmountMicro,
		req.LedgerIdempotencyKey,
		paymentIntentID,
		req.Provider,
		req.ProviderRef,
	)
	if err != nil {
		if errors.Is(err, ErrCustomerNotFound) {
			return nil, status.Error(codes.NotFound, "customer not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to apply payment credit: %v", err)
	}

	return &pb.ApplyPaymentCreditResponse{
		Applied:       applied,
		LedgerEntryId: ledgerEntryID,
	}, nil
}

func (h *SettlementHandler) ApplyPaymentRefund(ctx context.Context, req *pb.ApplyPaymentRefundRequest) (*pb.ApplyPaymentRefundResponse, error) {
	if err := h.requireSettlementToken(ctx); err != nil {
		return nil, err
	}

	customerID, err := uuid.Parse(req.CustomerId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid customer id")
	}
	paymentIntentID, err := uuid.Parse(req.PaymentIntentId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payment intent id")
	}
	if req.AmountMicro <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_micro must be positive")
	}

	applied, ledgerEntryID, err := h.service.ApplyPaymentRefund(
		ctx,
		customerID,
		req.AmountMicro,
		req.LedgerIdempotencyKey,
		paymentIntentID,
		req.Provider,
		req.ProviderRefundId,
	)
	if err != nil {
		if errors.Is(err, ErrCustomerNotFound) {
			return nil, status.Error(codes.NotFound, "customer not found")
		}
		if errors.Is(err, ErrPaymentTopupNotFound) {
			return nil, status.Error(codes.NotFound, "payment topup not found")
		}
		if errors.Is(err, ErrRefundExceedsTopup) {
			return nil, status.Error(codes.FailedPrecondition, "refund exceeds settled topup")
		}
		return nil, status.Errorf(codes.Internal, "failed to apply payment refund: %v", err)
	}

	return &pb.ApplyPaymentRefundResponse{
		Applied:       applied,
		LedgerEntryId: ledgerEntryID,
	}, nil
}

func (h *SettlementHandler) ApplyPaymentChargeback(ctx context.Context, req *pb.ApplyPaymentChargebackRequest) (*pb.ApplyPaymentChargebackResponse, error) {
	if err := h.requireSettlementToken(ctx); err != nil {
		return nil, err
	}
	customerID, paymentIntentID, err := parseSettlementCustomerAndIntent(req.GetCustomerId(), req.GetPaymentIntentId())
	if err != nil {
		return nil, err
	}
	if req.AmountMicro <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_micro must be positive")
	}

	applied, ledgerEntryID, err := h.service.ApplyPaymentChargeback(
		ctx, customerID, req.AmountMicro, req.LedgerIdempotencyKey, paymentIntentID, req.Provider, req.ProviderDisputeId,
	)
	if err != nil {
		return nil, h.mapChargebackError(err)
	}
	return &pb.ApplyPaymentChargebackResponse{Applied: applied, LedgerEntryId: ledgerEntryID}, nil
}

func (h *SettlementHandler) ApplyPaymentChargebackReversal(ctx context.Context, req *pb.ApplyPaymentChargebackReversalRequest) (*pb.ApplyPaymentChargebackReversalResponse, error) {
	if err := h.requireSettlementToken(ctx); err != nil {
		return nil, err
	}
	customerID, paymentIntentID, err := parseSettlementCustomerAndIntent(req.GetCustomerId(), req.GetPaymentIntentId())
	if err != nil {
		return nil, err
	}
	if req.AmountMicro <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_micro must be positive")
	}

	applied, ledgerEntryID, err := h.service.ApplyPaymentChargebackReversal(
		ctx, customerID, req.AmountMicro, req.LedgerIdempotencyKey, paymentIntentID, req.Provider, req.ProviderDisputeId,
	)
	if err != nil {
		return nil, h.mapChargebackError(err)
	}
	return &pb.ApplyPaymentChargebackReversalResponse{Applied: applied, LedgerEntryId: ledgerEntryID}, nil
}

func (h *SettlementHandler) GetLedgerEntry(ctx context.Context, req *pb.GetLedgerEntryRequest) (*pb.GetLedgerEntryResponse, error) {
	if err := h.requireSettlementToken(ctx); err != nil {
		return nil, err
	}
	paymentIntentID, err := uuid.Parse(req.GetPaymentIntentId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payment intent id")
	}

	found, entry, refundTotal, chargebackTotal, reversalTotal, err := h.service.GetLedgerEntry(ctx, paymentIntentID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to load ledger entry: %v", err)
	}

	resp := &pb.GetLedgerEntryResponse{
		Found:                        found,
		RefundTotalMicro:             refundTotal,
		ChargebackTotalMicro:         chargebackTotal,
		ChargebackReversalTotalMicro: reversalTotal,
	}
	if found {
		campID := ""
		if entry.CampaignID.Valid {
			campID = uuid.UUID(entry.CampaignID.Bytes).String()
		}
		resp.Topup = &pb.LedgerEntry{
			Id:          entry.ID,
			CustomerId:  uuid.UUID(entry.CustomerID.Bytes).String(),
			CampaignId:  campID,
			AmountMicro: entry.Amount,
			Type:        string(entry.Type),
			CreatedAt:   entry.CreatedAt.Time.UTC().Format(time.RFC3339),
		}
	}
	return resp, nil
}

func (h *SettlementHandler) requireSettlementToken(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	tokens := md.Get("x-internal-token")
	expectedToken := string(h.cfg.SettlementInternalToken)
	if expectedToken == "" {
		return status.Error(codes.FailedPrecondition, "settlement internal token not configured")
	}
	if len(tokens) == 0 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(expectedToken)) != 1 {
		return status.Error(codes.PermissionDenied, "invalid internal token")
	}
	return nil
}

func parseSettlementCustomerAndIntent(customerIDStr, intentIDStr string) (uuid.UUID, uuid.UUID, error) {
	customerID, err := uuid.Parse(customerIDStr)
	if err != nil {
		return uuid.Nil, uuid.Nil, status.Error(codes.InvalidArgument, "invalid customer id")
	}
	paymentIntentID, err := uuid.Parse(intentIDStr)
	if err != nil {
		return uuid.Nil, uuid.Nil, status.Error(codes.InvalidArgument, "invalid payment intent id")
	}
	return customerID, paymentIntentID, nil
}

func (h *SettlementHandler) mapChargebackError(err error) error {
	if errors.Is(err, ErrCustomerNotFound) {
		return status.Error(codes.NotFound, "customer not found")
	}
	if errors.Is(err, ErrPaymentTopupNotFound) {
		return status.Error(codes.NotFound, "payment topup not found")
	}
	if errors.Is(err, ErrChargebackExceedsTopup) {
		return status.Error(codes.FailedPrecondition, "chargeback exceeds settled topup")
	}
	if errors.Is(err, ErrChargebackReversalExceedsWithdrawn) {
		return status.Error(codes.FailedPrecondition, "chargeback reversal exceeds withdrawn amount")
	}
	return status.Errorf(codes.Internal, "failed to apply payment chargeback: %v", err)
}
