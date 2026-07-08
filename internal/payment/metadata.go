package payment

import (
	"espx/internal/payment/db"
	"espx/pkg/cold"
)

const metadataCheckoutURLKey = "checkout_url"

// mergeIntentMetadata persists the checkout redirect on the intent so idempotent retries
// can return the same URL without calling the provider again.
func mergeIntentMetadata(base map[string]string, checkoutURL string) ([]byte, error) {
	meta := make(map[string]string, len(base)+1)
	for k, v := range base {
		meta[k] = v
	}
	if checkoutURL != "" {
		meta[metadataCheckoutURLKey] = checkoutURL
	}
	return cold.MarshalJSON(meta)
}

// checkoutURLFromIntent recovers the stored redirect for idempotent create responses.
func checkoutURLFromIntent(intent db.PaymentPaymentIntent) string {
	var meta map[string]string
	if err := cold.UnmarshalJSON(intent.Metadata, &meta); err != nil {
		return ""
	}
	return meta[metadataCheckoutURLKey]
}
