package payment

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mapPaymentGRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrInvalidCustomerID),
		errors.Is(err, ErrInvalidIntentID),
		errors.Is(err, ErrInvalidAmount),
		errors.Is(err, ErrInvalidRequestBody):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrCustomerNotFound),
		errors.Is(err, ErrPaymentIntentNotFound),
		errors.Is(err, ErrWebhookEventNotFound),
		errors.Is(err, pgx.ErrNoRows):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "idempotency key conflict")
	case errors.Is(err, ErrProviderNotConfigured):
		return status.Error(codes.FailedPrecondition, "stripe checkout not configured")
	case errors.Is(err, ErrCheckoutUnavailable):
		return status.Error(codes.Unavailable, "checkout unavailable")
	default:
		return status.Error(codes.Internal, "internal server error")
	}
}
