package management

import (
	"context"
	"crypto/subtle"
	"errors"

	"espx/internal/config"
	"espx/internal/management/pb"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// SettlementHandler serves internal payment outbox workers that credit customer balances.
type SettlementHandler struct {
	pb.UnimplementedSettlementServiceServer
	service *Service
	cfg     *config.Config
}

// NewSettlementHandler exposes ledger credit on a dedicated gRPC port so payment outbox does not use admin HTTP.
func NewSettlementHandler(service *Service, cfg *config.Config) *SettlementHandler {
	return &SettlementHandler{
		service: service,
		cfg:     cfg,
	}
}

// ApplyPaymentCredit validates the internal token then credits the ledger idempotently for one succeeded intent.
func (h *SettlementHandler) ApplyPaymentCredit(ctx context.Context, req *pb.ApplyPaymentCreditRequest) (*pb.ApplyPaymentCreditResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	tokens := md.Get("x-internal-token")
	expectedToken := string(h.cfg.SettlementInternalToken)
	if expectedToken == "" {
		return nil, status.Error(codes.FailedPrecondition, "settlement internal token not configured")
	}
	if len(tokens) == 0 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(expectedToken)) != 1 {
		return nil, status.Error(codes.PermissionDenied, "invalid internal token")
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
