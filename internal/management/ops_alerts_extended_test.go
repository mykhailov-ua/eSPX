package management

import (
	"context"
	"testing"
	"time"

	notifierpb "espx/internal/notifier/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpsAlerter_AlertOutboxStuck(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)
	assert.Equal(t, 120, alerter.OutboxStuckThresholdSec())

	alerter.AlertOutboxStuck(12, 180)
	time.Sleep(100 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST, requests[0].DeliveryMode)
}

func TestOpsAlerter_AlertCHEmergencyDrop(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	alerter.AlertCHEmergencyDrop("impressions", "202401", 92.5, 90)
	time.Sleep(100 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Contains(t, requests[0].Body, "CH emergency drop")
	assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST, requests[0].DeliveryMode)
}

func TestOpsAlerter_AlertBlacklistJanitorFailed(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	alerter.AlertBlacklistJanitorFailed(assert.AnError)
	time.Sleep(100 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Contains(t, requests[0].Body, "Blacklist janitor")
}

func TestOpsAlerter_AlertSlotMigrationError(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	alerter.AlertSlotMigrationError("copy", assert.AnError)
	time.Sleep(100 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Contains(t, requests[0].Title, "slot migration copy failed")
}

func TestOutboxMetrics_AlertsWhenStale(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true
	cfg.Management.OpsAlertOutboxStuckSec = 60

	svc := &Service{alerter: NewOpsAlerter(&NotifierClient{client: stub}, cfg)}
	worker := &OutboxWorker{svc: svc}

	worker.recordOutboxLagFromValues(5, 90)
	time.Sleep(100 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Contains(t, requests[0].Body, "Outbox backlog stale")
}

func TestOpsAlerter_EnqueueFailureMetaAlert(t *testing.T) {
	stub := &stubNotifierGRPCClient{fail: true}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	err := alerter.enqueueNotification(context.Background(), "test", "test", "body", false)
	require.Error(t, err)
}

// TestChaos_opsAlertExtendedCoverage verifies new ops alert paths enqueue broadcast notifications.
func TestChaos_opsAlertExtendedCoverage(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	alerter.AlertBlacklistJanitorFailed(assert.AnError)
	alerter.AlertOutboxStuck(3, 150)
	alerter.AlertSlotMigrationError("drain", assert.AnError)
	time.Sleep(150 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 3)
	for _, req := range requests {
		assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST, req.DeliveryMode)
	}

	logChaosProof(t, "ops_alert_extended", map[string]string{
		"blacklist": "true",
		"outbox":    "true",
		"migration": "true",
		"broadcast": "true",
	})
}
