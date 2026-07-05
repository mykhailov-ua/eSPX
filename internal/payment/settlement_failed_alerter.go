package payment

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"espx/internal/config"
	"espx/internal/metrics"
	"espx/internal/payment/db"

	notifierpb "espx/internal/notifier/pb"
)

// SettlementFailedAlerter notifies operators when payment outbox settlement permanently fails.
type SettlementFailedAlerter struct {
	client             *NotifierClient
	provider           notifierpb.Provider
	recipient          string
	broadcastProviders []notifierpb.Provider
	cooldown           time.Duration
	lastSent           sync.Map // payment_intent_id -> time.Time
}

// NewSettlementFailedAlerter constructs an alerter when OPS_ALERTS_ENABLED and a recipient are set.
func NewSettlementFailedAlerter(client *NotifierClient, cfg *config.Config) *SettlementFailedAlerter {
	if client == nil || cfg == nil || !cfg.OpsAlertsEnabled() {
		return nil
	}
	provider, recipient, ok := resolveOpsAlertTarget(cfg)
	if !ok {
		return nil
	}
	cooldownSec := cfg.Management.OpsAlertCooldownSec
	if cooldownSec <= 0 {
		cooldownSec = 300
	}
	return &SettlementFailedAlerter{
		client:             client,
		provider:           provider,
		recipient:          recipient,
		broadcastProviders: resolveBroadcastProviders(cfg),
		cooldown:           time.Duration(cooldownSec) * time.Second,
	}
}

func (a *SettlementFailedAlerter) shouldSend(paymentIntentID string) bool {
	if a == nil || paymentIntentID == "" {
		return false
	}
	now := time.Now()
	if v, ok := a.lastSent.Load(paymentIntentID); ok {
		if now.Sub(v.(time.Time)) < a.cooldown {
			return false
		}
	}
	a.lastSent.Store(paymentIntentID, now)
	return true
}

// AlertPermanentFailure enqueues a notifier message for a terminal outbox settlement failure.
func (a *SettlementFailedAlerter) AlertPermanentFailure(outboxEvent db.PaymentPaymentOutbox, cause error) {
	if a == nil {
		return
	}
	intentID, ok := paymentIntentIDFromOutbox(outboxEvent)
	if !ok {
		return
	}
	if !a.shouldSend(intentID) {
		return
	}

	dedupKey := fmt.Sprintf("payment-settlement-failed:%s", intentID)
	title := "eSPX: payment settlement failed"
	body := formatSettlementFailedAlertBody(outboxEvent, intentID, cause)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := enqueueOpsNotification(ctx, a.client, a.provider, a.recipient, title, body, dedupKey, true, a.broadcastProviders); err != nil {
			metrics.ManagementOpsAlertEnqueueFailuresTotal.Inc()
			slog.Warn("payment settlement failed alert enqueue failed", "intent_id", intentID, "error", err)
		}
	}()
}

func paymentIntentIDFromOutbox(outboxEvent db.PaymentPaymentOutbox) (string, bool) {
	switch outboxEvent.EventType {
	case "SETTLE_BALANCE":
		var payload SettleBalancePayload
		if err := json.Unmarshal(outboxEvent.Payload, &payload); err != nil || payload.PaymentIntentID == "" {
			return "", false
		}
		return payload.PaymentIntentID, true
	case OutboxEventReverseBalance:
		var payload ReverseBalancePayload
		if err := json.Unmarshal(outboxEvent.Payload, &payload); err != nil || payload.PaymentIntentID == "" {
			return "", false
		}
		return payload.PaymentIntentID, true
	case OutboxEventApplyChargeback, OutboxEventReverseChargeback:
		var payload struct {
			PaymentIntentID string `json:"payment_intent_id"`
		}
		if err := json.Unmarshal(outboxEvent.Payload, &payload); err != nil || payload.PaymentIntentID == "" {
			return "", false
		}
		return payload.PaymentIntentID, true
	default:
		return "", false
	}
}

func formatSettlementFailedAlertBody(outboxEvent db.PaymentPaymentOutbox, intentID string, cause error) string {
	errText := ""
	if cause != nil {
		errText = cause.Error()
	}
	return fmt.Sprintf(
		"<b>Payment settlement failed</b>\nIntent: %s\nOutbox #%d (%s)\nError: %s",
		intentID, outboxEvent.ID, outboxEvent.EventType, errText,
	)
}
