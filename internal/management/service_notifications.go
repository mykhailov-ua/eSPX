package management

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RetryNotification resets a FAILED notifier row to PENDING for operator replay.
func (s *Service) RetryNotification(ctx context.Context, notificationID string) error {
	id, err := uuid.Parse(notificationID)
	if err != nil {
		return fmt.Errorf("invalid notification id: %w", err)
	}
	tag, err := s.GetPool().Exec(ctx, `
		UPDATE notifier.notifications
		SET status = 'PENDING',
		    retry_count = 0,
		    error_message = NULL,
		    claimed_at = NULL,
		    updated_at = now()
		WHERE id = $1 AND status = 'FAILED'`, id)
	if err != nil {
		return fmt.Errorf("retry notification: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.Join(pgx.ErrNoRows, fmt.Errorf("notification not in FAILED state"))
	}
	return nil
}
