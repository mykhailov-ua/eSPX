package payment

import (
	"context"
	"crypto/subtle"
	"errors"
	"espx/internal/config"
	"espx/internal/payment/db"
	"espx/internal/payment/pb"
	"espx/pkg/cold"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Handler is the gRPC adapter; auth runs here because payment has no end-user identity layer.
type Handler struct {
	pb.UnimplementedPaymentServiceServer
	service *Service
	cfg     *config.Config
}

// NewHandler exposes payment operations over gRPC because only trusted internal services
// may create intents; public checkout traffic uses the HTTP sidecar instead.
func NewHandler(service *Service, cfg *config.Config) *Handler {
	return &Handler{
		service: service,
		cfg:     cfg,
	}
}

// requireInternalToken gates gRPC before any money-bearing work because the service has no end-user auth layer.
func (h *Handler) requireInternalToken(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	tokens := md.Get("x-internal-token")
	expectedToken := string(h.cfg.PaymentInternalToken)
	if expectedToken == "" {
		return status.Error(codes.FailedPrecondition, "payment internal token not configured")
	}
	if len(tokens) == 0 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(expectedToken)) != 1 {
		return status.Error(codes.PermissionDenied, "invalid internal token")
	}
	return nil
}

// mapStatusToPB keeps sqlc-generated DB enums out of the protobuf API contract.
func mapStatusToPB(s db.PaymentPaymentIntentStatus) pb.PaymentIntentStatus {
	switch s {
	case db.PaymentPaymentIntentStatusCREATED:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_CREATED
	case db.PaymentPaymentIntentStatusPENDINGPROVIDER:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_PENDING_PROVIDER
	case db.PaymentPaymentIntentStatusPROCESSING:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_PROCESSING
	case db.PaymentPaymentIntentStatusSUCCEEDED:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_SUCCEEDED
	case db.PaymentPaymentIntentStatusFAILED:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_FAILED
	case db.PaymentPaymentIntentStatusCANCELLED:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_CANCELLED
	case db.PaymentPaymentIntentStatusREFUNDED:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_REFUNDED
	case db.PaymentPaymentIntentStatusSETTLEMENTFAILED:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_SETTLEMENT_FAILED
	default:
		return pb.PaymentIntentStatus_PAYMENT_INTENT_STATUS_UNSPECIFIED
	}
}

func intentToPB(intent db.PaymentPaymentIntent) *pb.PaymentIntent {
	return &pb.PaymentIntent{
		Id:             uuid.UUID(intent.ID.Bytes).String(),
		CustomerId:     uuid.UUID(intent.CustomerID.Bytes).String(),
		AmountMicro:    intent.AmountMicro,
		Currency:       intent.Currency,
		Status:         mapStatusToPB(intent.Status),
		Provider:       intent.Provider,
		ProviderRef:    intent.ProviderRef.String,
		IdempotencyKey: intent.IdempotencyKey,
		CreatedAt:      timestamppb.New(intent.CreatedAt.Time),
		UpdatedAt:      timestamppb.New(intent.UpdatedAt.Time),
	}
}

// CreatePaymentIntent validates wire input here so invalid amounts never reach the idempotent service layer.
func (handler *Handler) CreatePaymentIntent(ctx context.Context, req *pb.CreatePaymentIntentRequest) (*pb.CreatePaymentIntentResponse, error) {
	if err := handler.requireInternalToken(ctx); err != nil {
		return nil, err
	}

	customerID, err := uuid.Parse(req.CustomerId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid customer id")
	}
	if req.AmountMicro <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_micro must be greater than zero")
	}
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	currency := req.Currency
	if currency == "" {
		currency = "USD"
	}

	result, err := handler.service.CreatePaymentIntent(ctx, customerID, req.AmountMicro, currency, req.IdempotencyKey, req.Metadata)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "customer not found")
		}
		if errors.Is(err, ErrProviderNotConfigured) || strings.Contains(err.Error(), ErrProviderNotConfigured.Error()) {
			return nil, status.Error(codes.FailedPrecondition, "stripe checkout not configured")
		}
		if strings.Contains(err.Error(), "idempotency key conflict") {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "payment intent creation failed: %v", err)
	}

	intent := result.Intent

	return &pb.CreatePaymentIntentResponse{
		IntentId:    uuid.UUID(intent.ID.Bytes).String(),
		Status:      mapStatusToPB(intent.Status),
		CheckoutUrl: result.CheckoutURL,
		ProviderRef: intent.ProviderRef.String,
	}, nil
}

// GetPaymentIntent serves post-checkout polling without exposing the full customer intent list.
func (handler *Handler) GetPaymentIntent(ctx context.Context, req *pb.GetPaymentIntentRequest) (*pb.PaymentIntent, error) {
	if err := handler.requireInternalToken(ctx); err != nil {
		return nil, err
	}

	intentID, err := uuid.Parse(req.IntentId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid intent id")
	}

	intent, err := handler.service.GetPaymentIntent(ctx, intentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "payment intent not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to get payment intent: %v", err)
	}

	return intentToPB(intent), nil
}

// ListPaymentIntents supports support and billing review without unbounded scans on the hot path.
func (handler *Handler) ListPaymentIntents(ctx context.Context, req *pb.ListPaymentIntentsRequest) (*pb.ListPaymentIntentsResponse, error) {
	if err := handler.requireInternalToken(ctx); err != nil {
		return nil, err
	}

	customerID, err := uuid.Parse(req.CustomerId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid customer id")
	}

	limit, offset := cold.ClampLimitOffset(req.Limit, req.Offset, 10, 100)

	intents, total, err := handler.service.ListPaymentIntents(ctx, customerID, limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list payment intents: %v", err)
	}

	return &pb.ListPaymentIntentsResponse{
		Intents: cold.MapSlice(intents, intentToPB),
		Total:   total,
	}, nil
}
