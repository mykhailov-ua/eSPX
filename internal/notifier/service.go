// Package notifier enqueues outbound alerts in Postgres and delivers them asynchronously via Telegram, Slack, or SMTP.
package notifier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"espx/internal/notifier/db"
	"espx/internal/notifier/pb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Service owns notification persistence and the background delivery loop.
type Service struct {
	pool      *pgxpool.Pool
	queries   *db.Queries
	providers map[pb.Provider]Provider
}

// NewService binds Postgres and delivery providers for gRPC enqueue and worker dispatch.
func NewService(pool *pgxpool.Pool, providers map[pb.Provider]Provider) *Service {
	return &Service{
		pool:      pool,
		queries:   db.New(pool),
		providers: providers,
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

	dbProvider, err := MapPBProviderToDB(req.Provider)
	if err != nil {
		return nil, err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate notification id: %w", err)
	}

	var title pgtype.Text
	if req.Title != "" {
		title = pgtype.Text{String: req.Title, Valid: true}
	}

	notification, err := service.queries.CreateNotification(ctx, db.CreateNotificationParams{
		ID:        pgtype.UUID{Bytes: id, Valid: true},
		Provider:  dbProvider,
		Recipient: req.Recipient,
		Title:     title,
		Body:      req.Body,
	})
	if err != nil {
		return nil, fmt.Errorf("enqueue notification: %w", err)
	}

	return &pb.SendNotificationResponse{
		NotificationId: uuid.UUID(notification.ID.Bytes).String(),
		Status:         MapDBStatusToPB(notification.Status),
	}, nil
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

// ProcessPending claims a batch of due PENDING rows and attempts delivery inside one transaction.
func (service *Service) ProcessPending(ctx context.Context, batchSize int32) (int, error) {
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("start transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := service.queries.WithTx(tx)

	notifications, err := qtx.GetPendingNotificationsForUpdate(ctx, batchSize)
	if err != nil {
		return 0, fmt.Errorf("fetch pending notifications: %w", err)
	}

	if len(notifications) == 0 {
		return 0, nil
	}

	// 1. Group / Deduplicate & Correlate notifications in-memory
	type groupKey struct {
		provider  db.NotifierProvider
		recipient string
		title     string
	}

	groups := make(map[groupKey][]db.NotifierNotification)
	var orderedKeys []groupKey

	for _, n := range notifications {
		key := groupKey{
			provider:  n.Provider,
			recipient: n.Recipient,
			title:     n.Title.String,
		}
		if _, exists := groups[key]; !exists {
			orderedKeys = append(orderedKeys, key)
		}
		groups[key] = append(groups[key], n)
	}

	processedCount := 0

	// Define Multi-Channel Fallback chain: Slack -> Telegram -> SMS -> SMTP
	fallbackChain := map[db.NotifierProvider]db.NotifierProvider{
		db.NotifierProviderSLACK:    db.NotifierProviderTELEGRAM,
		db.NotifierProviderTELEGRAM: db.NotifierProviderSMS,
		db.NotifierProviderSMS:      db.NotifierProviderSMTP,
	}

	for _, key := range orderedKeys {
		items := groups[key]
		lead := items[0]
		leadID := uuid.UUID(lead.ID.Bytes).String()

		// Build the body (possibly aggregated if more than 1 item in group)
		var finalBody string
		isAggregated := len(items) > 1

		if isAggregated {
			var sb strings.Builder
			sb.WriteString(lead.Body)
			sb.WriteString("\n\n---\n")
			sb.WriteString(fmt.Sprintf("⚠️ [DEDUPLICATED] Accumulated %d similar events.\n", len(items)))

			firstTime := lead.CreatedAt.Time
			lastTime := lead.CreatedAt.Time
			for _, item := range items {
				if item.CreatedAt.Time.Before(firstTime) {
					firstTime = item.CreatedAt.Time
				}
				if item.CreatedAt.Time.After(lastTime) {
					lastTime = item.CreatedAt.Time
				}
			}
			sb.WriteString(fmt.Sprintf("First seen: %s\n", firstTime.Format(time.RFC3339)))
			sb.WriteString(fmt.Sprintf("Last seen: %s\n", lastTime.Format(time.RFC3339)))

			// Append unique bodies/details from other messages
			uniqueBodies := make(map[string]bool)
			for _, item := range items {
				uniqueBodies[item.Body] = true
			}
			if len(uniqueBodies) > 1 {
				sb.WriteString("\nUnique event details:\n")
				detailsCount := 0
				for b := range uniqueBodies {
					if b != lead.Body {
						sb.WriteString(fmt.Sprintf("- %s\n", b))
						detailsCount++
						if detailsCount >= 5 {
							sb.WriteString("- ... (truncated)\n")
							break
						}
					}
				}
			}
			finalBody = sb.String()
		} else {
			finalBody = lead.Body
		}

		// 2. Attempt delivery with Multi-Channel Fallback
		currentProvider := lead.Provider
		currentRecipient := lead.Recipient
		var sendErr error
		var sentProvider db.NotifierProvider = currentProvider

		for {
			pbProvider := MapDBProviderToPB(currentProvider)
			provider, exists := service.providers[pbProvider]
			if !exists {
				sendErr = fmt.Errorf("provider %s not configured", currentProvider)
			} else {
				// Inject the notification ID into context for interactive buttons
				pCtx := context.WithValue(ctx, "notification_id", leadID)
				sendErr = provider.Send(pCtx, currentRecipient, lead.Title.String, finalBody)
				if sendErr == nil {
					sentProvider = currentProvider
					break
				}
			}

			// Find next configured fallback provider in the chain
			nextProvider := currentProvider
			var fallbackFound bool
			for {
				var ok bool
				nextProvider, ok = fallbackChain[nextProvider]
				if !ok {
					break
				}
				nextPB := MapDBProviderToPB(nextProvider)
				if _, configured := service.providers[nextPB]; configured {
					fallbackFound = true
					break
				}
			}

			if !fallbackFound {
				break
			}

			slog.Warn("notification delivery failed, attempting fallback",
				"notification_id", leadID,
				"failed_provider", currentProvider,
				"fallback_provider", nextProvider,
				"error", sendErr,
			)

			currentProvider = nextProvider
			currentRecipient = "" // Fallback provider will use its configured default recipient
		}

		// 3. Persist delivery outcomes
		if sendErr == nil {
			// Delivery succeeded (possibly via fallback)
			// Update lead notification to SENT (with the actual provider used!)
			dbSentProvider := db.NullNotifierProvider{
				NotifierProvider: sentProvider,
				Valid:            true,
			}
			if _, updateErr := qtx.UpdateNotificationStatus(ctx, db.UpdateNotificationStatusParams{
				ID:           lead.ID,
				Status:       db.NotifierNotificationStatusSENT,
				Provider:     dbSentProvider,
				RetryCount:   pgtype.Int4{Int32: lead.RetryCount, Valid: true},
				ErrorMessage: pgtype.Text{Valid: false},
			}); updateErr != nil {
				slog.Error("failed to update lead notification status to SENT", "error", updateErr, "notification_id", leadID)
			}

			// Update all other deduplicated notifications in this group directly to SENT as well
			if isAggregated {
				for i := 1; i < len(items); i++ {
					subItem := items[i]
					subItemID := uuid.UUID(subItem.ID.Bytes).String()
					if _, updateErr := qtx.UpdateNotificationStatus(ctx, db.UpdateNotificationStatusParams{
						ID:           subItem.ID,
						Status:       db.NotifierNotificationStatusSENT,
						Provider:     dbSentProvider,
						RetryCount:   pgtype.Int4{Int32: subItem.RetryCount, Valid: true},
						ErrorMessage: pgtype.Text{String: fmt.Sprintf("Deduplicated and aggregated into %s", leadID), Valid: true},
					}); updateErr != nil {
						slog.Error("failed to update sub notification status to SENT", "error", updateErr, "notification_id", subItemID)
					}
				}
			}
			processedCount += len(items)
		} else {
			// All attempts (including fallbacks) failed. Lead notification remains PENDING or FAILS permanently.
			nextRetryCount := lead.RetryCount + 1
			nextStatus := db.NotifierNotificationStatusPENDING
			if nextRetryCount >= maxDeliveryAttempts {
				nextStatus = db.NotifierNotificationStatusFAILED
				slog.Error("notification delivery failed permanently", "error", sendErr, "notification_id", leadID, "retries", nextRetryCount)
			} else {
				slog.Warn("notification delivery failed, will retry", "error", sendErr, "notification_id", leadID, "retries", nextRetryCount)
			}

			if _, updateErr := qtx.UpdateNotificationStatus(ctx, db.UpdateNotificationStatusParams{
				ID:           lead.ID,
				Status:       nextStatus,
				RetryCount:   pgtype.Int4{Int32: nextRetryCount, Valid: true},
				ErrorMessage: pgtype.Text{String: sendErr.Error(), Valid: true},
			}); updateErr != nil {
				slog.Error("failed to update lead notification status after failure", "error", updateErr, "notification_id", leadID)
			}

			// Do NOT update subItems in this group if lead failed. They will remain PENDING and be retried
			// (and likely grouped again) on the next worker polling iteration.
			processedCount += 1
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}

	return processedCount, nil
}
