package notifier

import (
	"context"
	"errors"

	"espx/internal/notifier/pb"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler is the gRPC boundary for enqueue and status lookup RPCs.
type Handler struct {
	pb.UnimplementedNotifierServiceServer
	service *Service
}

// NewHandler wires the domain service into the generated NotifierServiceServer interface.
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (handler *Handler) SendNotification(ctx context.Context, req *pb.SendNotificationRequest) (*pb.SendNotificationResponse, error) {
	resp, err := handler.service.SendNotification(ctx, req)
	return resp, mapRPCError(err)
}

func (handler *Handler) SendNotificationBatch(ctx context.Context, req *pb.SendNotificationBatchRequest) (*pb.SendNotificationBatchResponse, error) {
	resp, err := handler.service.SendNotificationBatch(ctx, req)
	return resp, mapRPCError(err)
}

func (handler *Handler) GetNotification(ctx context.Context, req *pb.GetNotificationRequest) (*pb.GetNotificationResponse, error) {
	resp, err := handler.service.GetNotification(ctx, req)
	return resp, mapRPCError(err)
}

func mapRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrRecipientRequired) ||
		errors.Is(err, ErrBodyRequired) ||
		errors.Is(err, ErrUnsupportedProvider) ||
		errors.Is(err, ErrInvalidNotificationID) ||
		errors.Is(err, ErrRateLimited) ||
		errors.Is(err, ErrBatchEmpty) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, ErrNotificationNotFound) || errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, ErrNotificationNotFound.Error())
	}
	return status.Error(codes.Internal, "internal server error")
}
