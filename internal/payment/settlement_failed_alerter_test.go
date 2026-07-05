package payment

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"espx/internal/payment/db"

	notifierpb "espx/internal/notifier/pb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettlementFailedAlerter_CooldownDedupByIntent(t *testing.T) {
	t.Parallel()
	cfg := testPaymentOpsConfig()
	cfg.Management.OpsAlertCooldownSec = 300

	alerter := NewSettlementFailedAlerter(&NotifierClient{}, cfg)
	require.NotNil(t, alerter)

	intentID := uuid.New().String()
	if !alerter.shouldSend(intentID) {
		t.Fatal("first send should pass")
	}
	if alerter.shouldSend(intentID) {
		t.Fatal("second send within cooldown should be suppressed")
	}
}

func TestSettlementFailedAlerter_AlertPermanentFailure_enqueues(t *testing.T) {
	stub := &stubPaymentNotifierClient{}
	cfg := testPaymentOpsConfig()
	alerter := NewSettlementFailedAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	intentID := uuid.New()
	payload, err := json.Marshal(SettleBalancePayload{
		PaymentIntentID: intentID.String(),
		CustomerID:      uuid.New().String(),
		AmountMicro:     5_000_000,
	})
	require.NoError(t, err)

	alerter.AlertPermanentFailure(db.PaymentPaymentOutbox{
		ID:        99,
		EventType: "SETTLE_BALANCE",
		Payload:   payload,
	}, fmt.Errorf("customer not found"))
	time.Sleep(150 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Equal(t, notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST, requests[0].DeliveryMode)
	assert.Equal(t, "payment-settlement-failed:"+intentID.String(), requests[0].DedupKey)
	assert.Contains(t, requests[0].Body, intentID.String())
}

func TestSettlementFailedAlerter_AlertPermanentFailure_dedupSecondCall(t *testing.T) {
	stub := &stubPaymentNotifierClient{}
	cfg := testPaymentOpsConfig()
	alerter := NewSettlementFailedAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	intentID := uuid.New()
	payload, err := json.Marshal(SettleBalancePayload{PaymentIntentID: intentID.String()})
	require.NoError(t, err)
	ev := db.PaymentPaymentOutbox{ID: 1, EventType: "SETTLE_BALANCE", Payload: payload}

	alerter.AlertPermanentFailure(ev, fmt.Errorf("first"))
	alerter.AlertPermanentFailure(ev, fmt.Errorf("second"))
	time.Sleep(150 * time.Millisecond)

	assert.Len(t, stub.snapshot(), 1)
}

func TestPaymentIntentIDFromOutbox_settleBalance(t *testing.T) {
	t.Parallel()
	intentID := uuid.New().String()
	payload, err := json.Marshal(SettleBalancePayload{PaymentIntentID: intentID})
	require.NoError(t, err)
	got, ok := paymentIntentIDFromOutbox(db.PaymentPaymentOutbox{EventType: "SETTLE_BALANCE", Payload: payload})
	require.True(t, ok)
	assert.Equal(t, intentID, got)
}
