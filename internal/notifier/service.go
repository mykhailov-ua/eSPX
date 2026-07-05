package notifier

import (
	"context"
	"errors"
	"fmt"

	"espx/internal/notifier/db"
	"espx/internal/notifier/pb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Service owns notification persistence and the background delivery loop.
type Service struct {
	pool        *pgxpool.Pool
	queries     *db.Queries
	providers   map[pb.Provider]Provider
	options     ServiceOptions
	rateLimiter *recipientRateLimiter
}

// NewService binds Postgres and delivery providers for gRPC enqueue and worker dispatch.
func NewService(pool *pgxpool.Pool, providers map[pb.Provider]Provider) *Service {
	return NewServiceWithOptions(pool, providers, defaultServiceOptions())
}

// NewServiceWithOptions binds Postgres with delivery tuning options.
func NewServiceWithOptions(pool *pgxpool.Pool, providers map[pb.Provider]Provider, opts ServiceOptions) *Service {
	return &Service{
		pool:        pool,
		queries:     db.New(pool),
		providers:   providers,
		options:     opts,
		rateLimiter: newRecipientRateLimiter(opts.RateLimitPerMinute),
	}
}

// SendNotification persists a PENDING row for asynchronous delivery by the worker.
func (service *Service) SendNotification(ctx context.Context, req *pb.SendNotificationRequest) (*pb.SendNotificationResponse, error) {
	if req.Recipient == "" {
		return nil, ErrRecipientRequired
	}
	if req.Body == "" {
		return nil, ErrBodyRequired
	}
	if service.rateLimiter != nil && !service.rateLimiter.allow(req.Recipient) {
		return nil, ErrRateLimited
	}

	if req.DedupKey != "" {
		if existing, ok, err := service.findActiveByDedupKey(ctx, req.DedupKey); err != nil {
			return nil, err
		} else if ok {
			return &pb.SendNotificationResponse{
				NotificationId: uuidString(existing.ID),
				Status:         MapDBStatusToPB(existing.Status),
				Deduplicated:   true,
			}, nil
		}
	}

	notification, err := service.createNotification(ctx, req)
	if err != nil {
		return nil, err
	}

	return &pb.SendNotificationResponse{
		NotificationId: uuidString(notification.ID),
		Status:         MapDBStatusToPB(notification.Status),
	}, nil
}

// SendNotificationBatch enqueues multiple notifications atomically per item.
func (service *Service) SendNotificationBatch(ctx context.Context, req *pb.SendNotificationBatchRequest) (*pb.SendNotificationBatchResponse, error) {
	if req == nil || len(req.Notifications) == 0 {
		return nil, ErrBatchEmpty
	}

	out := make([]*pb.SendNotificationResponse, 0, len(req.Notifications))
	for _, item := range req.Notifications {
		resp, err := service.SendNotification(ctx, item)
		if err != nil {
			return nil, fmt.Errorf("batch item failed: %w", err)
		}
		out = append(out, resp)
	}
	return &pb.SendNotificationBatchResponse{Notifications: out}, nil
}

// GetNotification returns the stored row including delivery status and retry metadata.
func (service *Service) GetNotification(ctx context.Context, req *pb.GetNotificationRequest) (*pb.GetNotificationResponse, error) {
	id, err := pgUUIDFromString(req.NotificationId)
	if err != nil {
		return nil, err
	}

	notification, err := service.queries.GetNotification(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotificationNotFound
		}
		return nil, fmt.Errorf("query notification: %w", err)
	}

	return &pb.GetNotificationResponse{
		Notification: notificationToProto(notification),
	}, nil
}

func (service *Service) findActiveByDedupKey(ctx context.Context, dedupKey string) (db.NotifierNotification, bool, error) {
	existing, err := service.queries.FindActiveNotificationByDedupKey(ctx, db.FindActiveNotificationByDedupKeyParams{
		DedupKey: pgtype.Text{String: dedupKey, Valid: true},
		Column2:  int64(service.options.dedupCooldown().Seconds()),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.NotifierNotification{}, false, nil
		}
		return db.NotifierNotification{}, false, fmt.Errorf("find active notification by dedup key: %w", err)
	}
	return existing, true, nil
}

func (service *Service) createNotification(ctx context.Context, req *pb.SendNotificationRequest) (db.NotifierNotification, error) {
	dbProvider, err := MapPBProviderToDB(req.Provider)
	if err != nil {
		return db.NotifierNotification{}, err
	}

	broadcastProviders, err := MapPBProvidersToDBStrings(req.BroadcastProviders)
	if err != nil {
		return db.NotifierNotification{}, err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return db.NotifierNotification{}, fmt.Errorf("generate notification id: %w", err)
	}

	var title pgtype.Text
	if req.Title != "" {
		title = pgtype.Text{String: req.Title, Valid: true}
	}
	var dedupKey pgtype.Text
	if req.DedupKey != "" {
		dedupKey = pgtype.Text{String: req.DedupKey, Valid: true}
	}

	notification, err := service.queries.CreateNotification(ctx, db.CreateNotificationParams{
		ID:                 pgtype.UUID{Bytes: id, Valid: true},
		Provider:           dbProvider,
		Recipient:          req.Recipient,
		Title:              title,
		Body:               req.Body,
		DeliveryMode:       MapPBDeliveryModeToDB(req.DeliveryMode),
		BroadcastProviders: broadcastProviders,
		DedupKey:           dedupKey,
	})
	if err != nil {
		return db.NotifierNotification{}, fmt.Errorf("enqueue notification: %w", err)
	}
	return notification, nil
}

func uuidString(id pgtype.UUID) string {
	return uuid.UUID(id.Bytes).String()
}

func pgtypeInt4(value int32) pgtype.Int4 {
	return pgtype.Int4{Int32: value, Valid: true}
}

func pgtypeText(value string) pgtype.Text {
	return pgtype.Text{String: value, Valid: true}
}

func pgtypeTextOptional(value string) pgtype.Text {
	if value == "" {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: value, Valid: true}
}
