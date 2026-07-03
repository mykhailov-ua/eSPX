package payment

import "fmt"

// stripeCentsPerMicro ties Stripe cent granularity to ledger amount_micro so webhook amounts
// can be compared to stored intents without floating point.
const stripeCentsPerMicro = 10_000

// StripeAmountToMicro normalizes provider webhook amounts into the same integer unit as the ledger.
func StripeAmountToMicro(stripeAmount int64) int64 {
	return stripeAmount * stripeCentsPerMicro
}

// MicroToStripeAmount rejects sub-cent intent amounts before a checkout session is created,
// because Stripe cannot represent them and mismatches would fail webhook reconciliation.
func MicroToStripeAmount(amountMicro int64) (int64, error) {
	if amountMicro%stripeCentsPerMicro != 0 {
		return 0, fmt.Errorf("amount_micro %d is not aligned to Stripe cent granularity", amountMicro)
	}
	return amountMicro / stripeCentsPerMicro, nil
}
