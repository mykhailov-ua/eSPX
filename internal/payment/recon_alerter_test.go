package payment

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/payment/db"

	notifierpb "espx/internal/notifier/pb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type stubPaymentNotifierClient struct {
	mu       sync.Mutex
	requests []*notifierpb.SendNotificationRequest
}

func (stub *stubPaymentNotifierClient) SendNotification(
	_ context.Context,
	in *notifierpb.SendNotificationRequest,
	_ ...grpc.CallOption,
) (*notifierpb.SendNotificationResponse, error) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.requests = append(stub.requests, in)
	return &notifierpb.SendNotificationResponse{NotificationId: "stub-id"}, nil
}

func (stub *stubPaymentNotifierClient) SendNotificationBatch(
	_ context.Context,
	in *notifierpb.SendNotificationBatchRequest,
	_ ...grpc.CallOption,
) (*notifierpb.SendNotificationBatchResponse, error) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	for _, item := range in.Notifications {
		stub.requests = append(stub.requests, item)
	}
	return &notifierpb.SendNotificationBatchResponse{}, nil
}

func (stub *stubPaymentNotifierClient) GetNotification(
	_ context.Context,
	_ *notifierpb.GetNotificationRequest,
	_ ...grpc.CallOption,
) (*notifierpb.GetNotificationResponse, error) {
	return nil, nil
}

func (stub *stubPaymentNotifierClient) snapshot() []*notifierpb.SendNotificationRequest {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	out := make([]*notifierpb.SendNotificationRequest, len(stub.requests))
	copy(out, stub.requests)
	return out
}

func testPaymentOpsConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Management.OpsAlertsEnabled = true
	cfg.Notifier.TelegramChatID = "-100123"
	cfg.Notifier.ServerHost = "127.0.0.1"
	cfg.Notifier.Port = "8085"
	return cfg
}

func TestFinancialFindingSeverity_mapping(t *testing.T) {
	t.Parallel()
	cases := []struct {
		kind db.PaymentFinancialFindingKind
		want FinancialFindingSeverity
	}{
		{db.PaymentFinancialFindingKindMISSINGLEDGERTOPUP, SeverityCritical},
		{db.PaymentFinancialFindingKindTOPUPAMOUNTMISMATCH, SeverityCritical},
		{db.PaymentFinancialFindingKindSETTLEMENTFAILEDINTENT, SeverityCritical},
		{db.PaymentFinancialFindingKindDEADOUTBOX, SeverityWarn},
		{db.PaymentFinancialFindingKindREFUNDLEDGERDRIFT, SeverityWarn},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, financialFindingSeverity(tc.kind), string(tc.kind))
	}
}

func TestFinancialReconAlerter_CooldownDedup(t *testing.T) {
	t.Parallel()
	cfg := testPaymentOpsConfig()
	cfg.Management.OpsAlertCooldownSec = 300

	alerter := NewFinancialReconAlerter(&NotifierClient{}, cfg)
	require.NotNil(t, alerter)

	if !alerter.shouldSend("payment-financial-recon:run:1") {
		t.Fatal("first send should pass")
	}
	if alerter.shouldSend("payment-financial-recon:run:1") {
		t.Fatal("second send within cooldown should be suppressed")
	}
}

func TestFinancialReconAlerter_AlertFindings_enqueuesWarnPlus(t *testing.T) {
	stub := &stubPaymentNotifierClient{}
	cfg := testPaymentOpsConfig()
	alerter := NewFinancialReconAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	summary := FinancialReconSummary{RunID: 42, IntentsChecked: 3}
	findings := []FinancialReconFinding{
		{Kind: db.PaymentFinancialFindingKindMISSINGLEDGERTOPUP, PaymentIntentID: uuid.New()},
	}

	alerter.AlertFindings(summary, findings)
	time.Sleep(150 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Equal(t, notifierpb.Provider_PROVIDER_TELEGRAM, requests[0].Provider)
	assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST, requests[0].DeliveryMode)
	assert.Contains(t, requests[0].Body, "MISSING_LEDGER_TOPUP")
	assert.Equal(t, "payment-financial-recon:run:42", requests[0].DedupKey)
}

func TestFinancialReconAlerter_AlertFindings_skipsCleanRun(t *testing.T) {
	stub := &stubPaymentNotifierClient{}
	cfg := testPaymentOpsConfig()
	alerter := NewFinancialReconAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	alerter.AlertFindings(FinancialReconSummary{RunID: 1}, nil)
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, stub.snapshot())
}

func TestNewFinancialReconAlerter_DisabledWithoutRecipient(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{}
	cfg.Management.OpsAlertsEnabled = true
	if NewFinancialReconAlerter(&NotifierClient{}, cfg) != nil {
		t.Fatal("expected nil without recipient")
	}
}

func TestFormatFinancialReconAlertBody_includesKinds(t *testing.T) {
	body := formatFinancialReconAlertBody(FinancialReconSummary{RunID: 7, IntentsChecked: 2}, []FinancialReconFinding{
		{Kind: db.PaymentFinancialFindingKindDEADOUTBOX},
		{Kind: db.PaymentFinancialFindingKindDEADOUTBOX},
	})
	assert.Contains(t, body, "DEAD_OUTBOX: 2")
	assert.Contains(t, body, fmt.Sprintf("Run #%d", 7))
}
