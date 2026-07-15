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

const batchSettlementMaxItems = 500

func (h *SettlementHandler) BlockIP(ctx context.Context, req *pb.BlockIPRequest) (*pb.BlockIPResponse, error) {
	if err := h.requireSettlementToken(ctx); err != nil {
		return nil, err
	}
	if req.GetIp() == "" {
		return nil, status.Error(codes.InvalidArgument, "ip required")
	}
	source := req.GetSource()
	if source == "" {
		source = "fraud"
	}
	if err := h.service.BlockIP(ctx, req.GetIp(), source); err != nil {
		return nil, status.Errorf(codes.Internal, "block ip: %v", err)
	}
	return &pb.BlockIPResponse{Enqueued: true}, nil
}

func (h *SettlementHandler) EnqueueFraudThreat(ctx context.Context, req *pb.EnqueueFraudThreatRequest) (*pb.EnqueueFraudThreatResponse, error) {
	if err := h.requireSettlementToken(ctx); err != nil {
		return nil, err
	}
	if req.GetIp() == "" {
		return nil, status.Error(codes.InvalidArgument, "ip required")
	}
	if req.GetCampaignId() == "" {
		return nil, status.Error(codes.InvalidArgument, "campaign_id required")
	}

	payload := FraudThreatPayload{
		Action:     req.GetAction(),
		IP:         req.GetIp(),
		CampaignID: req.GetCampaignId(),
		Score:      req.GetScore(),
		Boost:      req.GetBoost(),
		TTLSeconds: req.GetTtlSeconds(),
	}

	if err := h.service.EnqueueFraudThreat(ctx, payload); err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue ml threat: %v", err)
	}
	return &pb.EnqueueFraudThreatResponse{Enqueued: true}, nil
}

func (h *SettlementHandler) BatchApplySettlement(ctx context.Context, req *pb.BatchApplySettlementRequest) (*pb.BatchApplySettlementResponse, error) {
	if err := h.requireSettlementToken(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	total := len(req.Credits) + len(req.Refunds) + len(req.Chargebacks) + len(req.ChargebackReversals)
	if total == 0 {
		return nil, status.Error(codes.InvalidArgument, "batch empty")
	}
	if total > batchSettlementMaxItems {
		return nil, status.Errorf(codes.InvalidArgument, "batch exceeds %d items", batchSettlementMaxItems)
	}

	resp := &pb.BatchApplySettlementResponse{}
	for _, item := range req.Credits {
		creditResp, err := h.ApplyPaymentCredit(ctx, item)
		resp.CreditResults = append(resp.CreditResults, batchItemFromCredit(creditResp, err))
	}
	for _, item := range req.Refunds {
		refundResp, err := h.ApplyPaymentRefund(ctx, item)
		resp.RefundResults = append(resp.RefundResults, batchItemFromRefund(refundResp, err))
	}
	for _, item := range req.Chargebacks {
		cbResp, err := h.ApplyPaymentChargeback(ctx, item)
		resp.ChargebackResults = append(resp.ChargebackResults, batchItemFromChargeback(cbResp, err))
	}
	for _, item := range req.ChargebackReversals {
		revResp, err := h.ApplyPaymentChargebackReversal(ctx, item)
		resp.ChargebackReversalResults = append(resp.ChargebackReversalResults, batchItemFromChargebackReversal(revResp, err))
	}
	return resp, nil
}

func batchItemFromCredit(resp *pb.ApplyPaymentCreditResponse, err error) *pb.BatchSettlementItemResult {
	if err != nil {
		return &pb.BatchSettlementItemResult{Error: err.Error()}
	}
	return &pb.BatchSettlementItemResult{Applied: resp.GetApplied(), LedgerEntryId: resp.GetLedgerEntryId()}
}

func batchItemFromRefund(resp *pb.ApplyPaymentRefundResponse, err error) *pb.BatchSettlementItemResult {
	if err != nil {
		return &pb.BatchSettlementItemResult{Error: err.Error()}
	}
	return &pb.BatchSettlementItemResult{Applied: resp.GetApplied(), LedgerEntryId: resp.GetLedgerEntryId()}
}

func batchItemFromChargeback(resp *pb.ApplyPaymentChargebackResponse, err error) *pb.BatchSettlementItemResult {
	if err != nil {
		return &pb.BatchSettlementItemResult{Error: err.Error()}
	}
	return &pb.BatchSettlementItemResult{Applied: resp.GetApplied(), LedgerEntryId: resp.GetLedgerEntryId()}
}

func batchItemFromChargebackReversal(resp *pb.ApplyPaymentChargebackReversalResponse, err error) *pb.BatchSettlementItemResult {
	if err != nil {
		return &pb.BatchSettlementItemResult{Error: err.Error()}
	}
	return &pb.BatchSettlementItemResult{Applied: resp.GetApplied(), LedgerEntryId: resp.GetLedgerEntryId()}
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
