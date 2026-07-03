package notifier

import (
	"espx/internal/notifier/db"
	"espx/internal/notifier/pb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	default:
		return pb.NotificationStatus_NOTIFICATION_STATUS_UNSPECIFIED
	}
}

func notificationToProto(n db.NotifierNotification) *pb.Notification {
	out := &pb.Notification{
		Id:           uuid.UUID(n.ID.Bytes).String(),
		Provider:     MapDBProviderToPB(n.Provider),
		Recipient:    n.Recipient,
		Body:         n.Body,
		Status:       MapDBStatusToPB(n.Status),
		RetryCount:   n.RetryCount,
		ErrorMessage: n.ErrorMessage.String,
	}
	if n.Title.Valid {
		out.Title = n.Title.String
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
