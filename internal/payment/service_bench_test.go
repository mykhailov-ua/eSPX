package payment

import (
	"testing"

	"espx/internal/payment/db"
)

// BenchmarkIsValidTransition measures webhook state guard cost on the intent status hot path.
func BenchmarkIsValidTransition(b *testing.B) {
	statuses := []db.PaymentPaymentIntentStatus{
		db.PaymentPaymentIntentStatusCREATED,
		db.PaymentPaymentIntentStatusPENDINGPROVIDER,
		db.PaymentPaymentIntentStatusPROCESSING,
		db.PaymentPaymentIntentStatusSUCCEEDED,
		db.PaymentPaymentIntentStatusFAILED,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		from := statuses[i%len(statuses)]
		to := statuses[(i+1)%len(statuses)]
		_ = isValidTransition(from, to)
	}
}
