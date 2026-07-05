package payment

import "github.com/google/uuid"

// OutboxEventApplyChargeback debits the customer ledger when Stripe withdraws disputed funds.
const OutboxEventApplyChargeback = "APPLY_CHARGEBACK"

// OutboxEventReverseChargeback credits the customer ledger when Stripe reinstates won dispute funds.
const OutboxEventReverseChargeback = "REVERSE_CHARGEBACK"

func chargebackWithdrawnLedgerKey(providerDisputeID string) string {
	return "chargeback:withdrawn:" + providerDisputeID
}

func chargebackReinstatedLedgerKey(providerDisputeID string) string {
	return "chargeback:reinstated:" + providerDisputeID
}

// ApplyChargebackPayload is the outbox JSON contract for management ApplyPaymentChargeback.
type ApplyChargebackPayload struct {
	CustomerID           string `json:"customer_id"`
	AmountMicro          int64  `json:"amount_micro"`
	LedgerIdempotencyKey string `json:"ledger_idempotency_key"`
	PaymentIntentID      string `json:"payment_intent_id"`
	Provider             string `json:"provider"`
	ProviderDisputeID    string `json:"provider_dispute_id"`
}

// ReverseChargebackPayload is the outbox JSON contract for management ApplyPaymentChargebackReversal.
type ReverseChargebackPayload struct {
	CustomerID           string `json:"customer_id"`
	AmountMicro          int64  `json:"amount_micro"`
	LedgerIdempotencyKey string `json:"ledger_idempotency_key"`
	PaymentIntentID      string `json:"payment_intent_id"`
	Provider             string `json:"provider"`
	ProviderDisputeID    string `json:"provider_dispute_id"`
}

func applyChargebackPayload(intentID, customerID uuid.UUID, amountMicro int64, providerDisputeID string) ApplyChargebackPayload {
	return ApplyChargebackPayload{
		CustomerID:           customerID.String(),
		AmountMicro:          amountMicro,
		LedgerIdempotencyKey: chargebackWithdrawnLedgerKey(providerDisputeID),
		PaymentIntentID:      intentID.String(),
		Provider:             "stripe",
		ProviderDisputeID:    providerDisputeID,
	}
}

func reverseChargebackPayload(intentID, customerID uuid.UUID, amountMicro int64, providerDisputeID string) ReverseChargebackPayload {
	return ReverseChargebackPayload{
		CustomerID:           customerID.String(),
		AmountMicro:          amountMicro,
		LedgerIdempotencyKey: chargebackReinstatedLedgerKey(providerDisputeID),
		PaymentIntentID:      intentID.String(),
		Provider:             "stripe",
		ProviderDisputeID:    providerDisputeID,
	}
}
