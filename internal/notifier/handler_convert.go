package notifier

import (
	"context"
	"espx/internal/notifier/db"
	"espx/internal/notifier/pb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type contextKey string

// NotificationIDContextKey carries the lead notification ID for interactive provider buttons.
const NotificationIDContextKey contextKey = "notification_id"

// NotificationIDFromContext returns the notification ID injected by ProcessPending.
func NotificationIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(NotificationIDContextKey).(string)
	return id, ok
}

func MapPBDeliveryModeToDB(mode pb.DeliveryMode) db.NotifierDeliveryMode {
	switch mode {
	case pb.DeliveryMode_DELIVERY_MODE_BROADCAST:
		return db.NotifierDeliveryModeBROADCAST
	case pb.DeliveryMode_DELIVERY_MODE_FALLBACK, pb.DeliveryMode_DELIVERY_MODE_UNSPECIFIED:
		return db.NotifierDeliveryModeFALLBACK
	default:
		return db.NotifierDeliveryModeFALLBACK
	}
}

func MapDBDeliveryModeToPB(mode db.NotifierDeliveryMode) pb.DeliveryMode {
	switch mode {
	case db.NotifierDeliveryModeBROADCAST:
		return pb.DeliveryMode_DELIVERY_MODE_BROADCAST
	case db.NotifierDeliveryModeFALLBACK:
		return pb.DeliveryMode_DELIVERY_MODE_FALLBACK
	default:
		return pb.DeliveryMode_DELIVERY_MODE_UNSPECIFIED
	}
}

func MapDBProvidersToPB(providers []string) []pb.Provider {
	if len(providers) == 0 {
		return nil
	}
	out := make([]pb.Provider, 0, len(providers))
	for _, provider := range providers {
		out = append(out, MapDBProviderToPB(db.NotifierProvider(provider)))
	}
	return out
}

func MapPBProvidersToDBStrings(providers []pb.Provider) ([]string, error) {
	if len(providers) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(providers))
	for _, provider := range providers {
		dbProvider, err := MapPBProviderToDB(provider)
		if err != nil {
			return nil, err
		}
		out = append(out, string(dbProvider))
	}
	return out, nil
}

func MapDBProviderStringsToDB(providers []string) []db.NotifierProvider {
	if len(providers) == 0 {
		return nil
	}
	out := make([]db.NotifierProvider, 0, len(providers))
	for _, provider := range providers {
		out = append(out, db.NotifierProvider(provider))
	}
	return out
}

func MapPBProviderToDB(p pb.Provider) (db.NotifierProvider, error) {
	switch p {
	case pb.Provider_PROVIDER_TELEGRAM:
		return db.NotifierProviderTELEGRAM, nil
	case pb.Provider_PROVIDER_SLACK:
		return db.NotifierProviderSLACK, nil
	case pb.Provider_PROVIDER_SMTP:
		return db.NotifierProviderSMTP, nil
	case pb.Provider_PROVIDER_SMS:
		return db.NotifierProviderSMS, nil
	default:
		return "", ErrUnsupportedProvider
	}
}

func MapDBProviderToPB(p db.NotifierProvider) pb.Provider {
	switch p {
	case db.NotifierProviderTELEGRAM:
		return pb.Provider_PROVIDER_TELEGRAM
	case db.NotifierProviderSLACK:
		return pb.Provider_PROVIDER_SLACK
	case db.NotifierProviderSMTP:
		return pb.Provider_PROVIDER_SMTP
	case db.NotifierProviderSMS:
		return pb.Provider_PROVIDER_SMS
	default:
		return pb.Provider_PROVIDER_UNSPECIFIED
	}
}

func MapDBStatusToPB(s db.NotifierNotificationStatus) pb.NotificationStatus {
	switch s {
	case db.NotifierNotificationStatusPENDING:
		return pb.NotificationStatus_NOTIFICATION_STATUS_PENDING
	case db.NotifierNotificationStatusSENT:
		return pb.NotificationStatus_NOTIFICATION_STATUS_SENT
	case db.NotifierNotificationStatusFAILED:
		return pb.NotificationStatus_NOTIFICATION_STATUS_FAILED
	case db.NotifierNotificationStatusPROCESSING:
		return pb.NotificationStatus_NOTIFICATION_STATUS_PROCESSING
	default:
		return pb.NotificationStatus_NOTIFICATION_STATUS_UNSPECIFIED
	}
}

func notificationToProto(n db.NotifierNotification) *pb.Notification {
	out := &pb.Notification{
		Id:                 uuid.UUID(n.ID.Bytes).String(),
		Provider:           MapDBProviderToPB(n.Provider),
		Recipient:          n.Recipient,
		Body:               n.Body,
		Status:             MapDBStatusToPB(n.Status),
		RetryCount:         n.RetryCount,
		ErrorMessage:       n.ErrorMessage.String,
		DeliveryMode:       MapDBDeliveryModeToPB(n.DeliveryMode),
		BroadcastProviders: MapDBProvidersToPB(n.BroadcastProviders),
	}
	if n.Title.Valid {
		out.Title = n.Title.String
	}
	if n.DedupKey.Valid {
		out.DedupKey = n.DedupKey.String
	}
	if n.CreatedAt.Valid {
		out.CreatedAt = timestamppb.New(n.CreatedAt.Time)
	}
	if n.UpdatedAt.Valid {
		out.UpdatedAt = timestamppb.New(n.UpdatedAt.Time)
	}
	return out
}

func pgUUIDFromString(id string) (pgtype.UUID, error) {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return pgtype.UUID{}, ErrInvalidNotificationID
	}
	return pgtype.UUID{Bytes: parsed, Valid: true}, nil
}
