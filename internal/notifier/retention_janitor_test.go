package notifier

import (
	"context"
	"testing"
	"time"

	"espx/internal/notifier/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionJanitor_DeletesOldRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	svc := NewService(pool, map[pb.Provider]Provider{})
	_, err := svc.SendNotification(ctx, &pb.SendNotificationRequest{
		Provider:  pb.Provider_PROVIDER_TELEGRAM,
		Recipient: "123",
		Title:     "old",
		Body:      "retention test",
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		UPDATE notifier.notifications
		SET status = 'SENT', created_at = NOW() - interval '40 days'
		WHERE status = 'PENDING'`)
	require.NoError(t, err)

	janitor := NewRetentionJanitor(pool, time.Hour, 30, 90)
	janitor.runOnce(ctx)

	var remaining int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM notifier.notifications`).Scan(&remaining)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
}
