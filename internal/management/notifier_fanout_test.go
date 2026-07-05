package management

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"espx/internal/config"
	notifierpb "espx/internal/notifier/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type stubNotifierGRPCClient struct {
	mu       sync.Mutex
	requests []*notifierpb.SendNotificationRequest
	fail     bool
}

func (stub *stubNotifierGRPCClient) SendNotification(
	ctx context.Context,
	in *notifierpb.SendNotificationRequest,
	opts ...grpc.CallOption,
) (*notifierpb.SendNotificationResponse, error) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.fail {
		return nil, fmt.Errorf("stub notifier unavailable")
	}
	stub.requests = append(stub.requests, in)
	return &notifierpb.SendNotificationResponse{NotificationId: "stub-id"}, nil
}

func (stub *stubNotifierGRPCClient) SendNotificationBatch(
	ctx context.Context,
	in *notifierpb.SendNotificationBatchRequest,
	opts ...grpc.CallOption,
) (*notifierpb.SendNotificationBatchResponse, error) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.fail {
		return nil, fmt.Errorf("stub notifier unavailable")
	}
	responses := make([]*notifierpb.SendNotificationResponse, 0, len(in.Notifications))
	for _, item := range in.Notifications {
		stub.requests = append(stub.requests, item)
		responses = append(responses, &notifierpb.SendNotificationResponse{NotificationId: "stub-id"})
	}
	return &notifierpb.SendNotificationBatchResponse{Notifications: responses}, nil
}

func (stub *stubNotifierGRPCClient) GetNotification(
	ctx context.Context,
	in *notifierpb.GetNotificationRequest,
	opts ...grpc.CallOption,
) (*notifierpb.GetNotificationResponse, error) {
	return nil, nil
}

func (stub *stubNotifierGRPCClient) snapshot() []*notifierpb.SendNotificationRequest {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	out := make([]*notifierpb.SendNotificationRequest, len(stub.requests))
	copy(out, stub.requests)
	return out
}

func testNotifierConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Env = "production"
	cfg.Notifier.TelegramChatID = "-100123"
	cfg.Notifier.SlackWebhookURL = "https://hooks.slack.com/services/test"
	cfg.Notifier.SMSDefaultRecipient = "+79001234567"
	return cfg
}

func TestResolveOpsAlertTargets_MultiChannel(t *testing.T) {
	targets := resolveOpsAlertTargets(testNotifierConfig())
	require.Len(t, targets, 3)
	assert.Equal(t, notifierpb.Provider_PROVIDER_TELEGRAM, targets[0].Provider)
	assert.Equal(t, notifierpb.Provider_PROVIDER_SLACK, targets[1].Provider)
	assert.Equal(t, notifierpb.Provider_PROVIDER_SMS, targets[2].Provider)
}

func TestResolveBroadcastProviders_AllConfigured(t *testing.T) {
	providers := resolveBroadcastProviders(testNotifierConfig())
	require.Len(t, providers, 3)
	assert.Equal(t, notifierpb.Provider_PROVIDER_TELEGRAM, providers[0])
	assert.Equal(t, notifierpb.Provider_PROVIDER_SLACK, providers[1])
	assert.Equal(t, notifierpb.Provider_PROVIDER_SMS, providers[2])
}

func TestAlertSeverityBroadcast(t *testing.T) {
	assert.True(t, alertSeverityBroadcast(AlertmanagerAlert{
		Labels: map[string]string{"severity": "critical"},
	}))
	assert.False(t, alertSeverityBroadcast(AlertmanagerAlert{
		Labels: map[string]string{"severity": "warning"},
	}))
	assert.False(t, alertSeverityBroadcast(AlertmanagerAlert{
		Labels: map[string]string{},
	}))
}

func TestAlertmanagerWebhook_CriticalUsesBroadcast(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.AlertmanagerWebhookEnabled = true

	h := &AlertmanagerWebhook{
		client:             &NotifierClient{client: stub},
		provider:           notifierpb.Provider_PROVIDER_TELEGRAM,
		recipient:          cfg.Notifier.TelegramChatID,
		broadcastProviders: resolveBroadcastProviders(cfg),
	}

	payload := AlertmanagerPayload{
		Alerts: []AlertmanagerAlert{{
			Status: "firing",
			Labels: map[string]string{
				"alertname": "HighErrorRate",
				"severity":  "critical",
			},
			Annotations: map[string]string{
				"summary":     "Tracker errors elevated",
				"description": "5xx ratio above SLO",
			},
			StartsAt: time.Now().UTC(),
		}},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/ops/alertmanager/webhook", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.handle(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST, requests[0].DeliveryMode)
	assert.Len(t, requests[0].BroadcastProviders, 3)
	assert.Equal(t, "alertmanager:HighErrorRate:firing", requests[0].DedupKey)
}

func TestAlertmanagerWebhook_WarningUsesFallback(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()

	h := &AlertmanagerWebhook{
		client:             &NotifierClient{client: stub},
		provider:           notifierpb.Provider_PROVIDER_TELEGRAM,
		recipient:          cfg.Notifier.TelegramChatID,
		broadcastProviders: resolveBroadcastProviders(cfg),
	}

	payload := AlertmanagerPayload{
		Alerts: []AlertmanagerAlert{{
			Status: "firing",
			Labels: map[string]string{
				"alertname": "LogCompactorHotLagHigh",
				"severity":  "warning",
			},
			Annotations: map[string]string{"summary": "Hot lag high"},
			StartsAt:    time.Now().UTC(),
		}},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/ops/alertmanager/webhook", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.handle(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_UNSPECIFIED, requests[0].DeliveryMode)
	assert.Empty(t, requests[0].BroadcastProviders)
}

func TestOpsAlerter_CriticalEventBroadcast(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	err := alerter.enqueueNotification(context.Background(), "recon", "recon", "body", true)
	require.NoError(t, err)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST, requests[0].DeliveryMode)
	assert.Len(t, requests[0].BroadcastProviders, 3)
}

func TestOpsAlerter_WarningEventFallback(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	err := alerter.enqueueNotification(context.Background(), "migration", "migration", "body", false)
	require.NoError(t, err)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_UNSPECIFIED, requests[0].DeliveryMode)
}

// TestChaos_alertmanagerWebhookFanOut verifies critical Prometheus alerts fan-out to all configured channels.
// Hypothesis: one critical webhook alert enqueues a single BROADCAST notification covering all configured providers.
func TestChaos_alertmanagerWebhookFanOut(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.AlertmanagerWebhookEnabled = true

	h := &AlertmanagerWebhook{
		client:             &NotifierClient{client: stub},
		provider:           notifierpb.Provider_PROVIDER_TELEGRAM,
		recipient:          cfg.Notifier.TelegramChatID,
		broadcastProviders: resolveBroadcastProviders(cfg),
	}

	const alertCount = 3
	alerts := make([]AlertmanagerAlert, 0, alertCount)
	for i := range alertCount {
		alerts = append(alerts, AlertmanagerAlert{
			Status: "firing",
			Labels: map[string]string{
				"alertname": "RedisInstanceDown",
				"severity":  "critical",
			},
			Annotations: map[string]string{"summary": "Redis down"},
			StartsAt:    time.Now().UTC(),
		})
		_ = i
	}

	payload := AlertmanagerPayload{Alerts: alerts}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/ops/alertmanager/webhook", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.handle(rec, req)

	requests := stub.snapshot()
	require.Len(t, requests, alertCount)
	for _, req := range requests {
		assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST, req.DeliveryMode)
		assert.Len(t, req.BroadcastProviders, 3)
	}

	logChaosProof(t, "alertmanager_webhook_fanout", map[string]string{
		"alerts":   "3",
		"channels": "3",
		"mode":     "BROADCAST",
		"severity": "critical",
	})
}

// TestChaos_opsEventFanOut verifies critical ops events enqueue broadcast notifications.
// Hypothesis: recon/redis/drain alerts use BROADCAST; migration alerts stay single-channel fallback.
func TestChaos_opsEventFanOut(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	alerter := NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	alerter.AlertReconDiscrepancy(42, 3, 1000, "2026-07-04")
	alerter.AlertRedisShardUnhealthy(1, assert.AnError)
	alerter.AlertDrainStuck(7, 3, "draining", "timeout", time.Now().UTC())
	alerter.AlertSlotMapMigrating(2, []int16{1, 2}, 0)

	time.Sleep(100 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 4)

	broadcastCount := 0
	fallbackCount := 0
	for _, req := range requests {
		if req.DeliveryMode == notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST {
			broadcastCount++
			assert.Len(t, req.BroadcastProviders, 3)
			continue
		}
		fallbackCount++
	}
	assert.Equal(t, 3, broadcastCount)
	assert.Equal(t, 1, fallbackCount)

	logChaosProof(t, "ops_event_fanout", map[string]string{
		"critical_events": "3",
		"warning_events":  "1",
		"channels":        "3",
		"broadcast_mode":  "true",
	})
}
