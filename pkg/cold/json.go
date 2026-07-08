package cold

import (
	"encoding/json"
	"fmt"
)

// MarshalJSON serializes v for outbox rows, audit blobs, or API-side hashing.
func MarshalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal json: %w", err)
	}
	return b, nil
}

// UnmarshalJSON decodes JSON payloads on cold paths (outbox, replicas, admin blobs).
func UnmarshalJSON(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("unmarshal json: %w", err)
	}
	return nil
}

// RedactStripeWebhookPayload strips sensitive Stripe fields before webhook persistence.
func RedactStripeWebhookPayload(payload []byte) ([]byte, error) {
	var redacted map[string]any
	if err := json.Unmarshal(payload, &redacted); err != nil {
		return nil, fmt.Errorf("unmarshal stripe webhook payload: %w", err)
	}
	delete(redacted, "client_secret")
	delete(redacted, "customer_details")
	return MarshalJSON(redacted)
}
