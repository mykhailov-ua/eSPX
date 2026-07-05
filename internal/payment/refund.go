package payment

import "github.com/google/uuid"

// refundLedgerIdempotencyKey ties one Stripe refund id to a single ledger debit row.
func refundLedgerIdempotencyKey(providerRefundID string) string {
	return "refund:" + providerRefundID
}

// OutboxEventReverseBalance is enqueued when a Stripe refund must debit the customer ledger.
const OutboxEventReverseBalance = "REVERSE_BALANCE"

// ReverseBalancePayload is the outbox JSON contract shared with management ApplyPaymentRefund.
type ReverseBalancePayload struct {
	CustomerID           string `json:"customer_id"`
	AmountMicro          int64  `json:"amount_micro"`
	LedgerIdempotencyKey string `json:"ledger_idempotency_key"`
	PaymentIntentID      string `json:"payment_intent_id"`
	Provider             string `json:"provider"`
	ProviderRefundID     string `json:"provider_refund_id"`
}

func reverseBalancePayload(intentID uuid.UUID, customerID uuid.UUID, amountMicro int64, providerRefundID string) ReverseBalancePayload {
	return ReverseBalancePayload{
		CustomerID:           customerID.String(),
		AmountMicro:          amountMicro,
		LedgerIdempotencyKey: refundLedgerIdempotencyKey(providerRefundID),
		PaymentIntentID:      intentID.String(),
		Provider:             "stripe",
		ProviderRefundID:     providerRefundID,
	}
}
