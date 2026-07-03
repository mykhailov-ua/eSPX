package billing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Guards VAT and sales tax rates apply to subtotals with basis-point rounding.
func TestTaxCalculator_Compute(t *testing.T) {
	calc := NewTaxCalculator()

	tests := []struct {
		name        string
		subtotal    int64
		profile     TaxProfile
		wantTax     int64
		wantRateBPS int32
	}{
		{
			name:     "none scheme",
			subtotal: 10_000_000,
			profile:  TaxProfile{Scheme: TaxSchemeNone},
			wantTax:  0,
		},
		{
			name:        "germany vat",
			subtotal:    10_000_000,
			profile:     TaxProfile{CountryCode: "DE", Scheme: TaxSchemeVAT},
			wantTax:     1_900_000,
			wantRateBPS: 1900,
		},
		{
			name:        "california sales tax",
			subtotal:    10_000_000,
			profile:     TaxProfile{CountryCode: "US", Scheme: TaxSchemeSalesTax, TaxRegion: "CA"},
			wantTax:     725_000,
			wantRateBPS: 725,
		},
		{
			name:        "explicit override bps",
			subtotal:    5_000_000,
			profile:     TaxProfile{Scheme: TaxSchemeVAT, RateBPS: 1500},
			wantTax:     750_000,
			wantRateBPS: 1500,
		},
		{
			name:     "zero subtotal",
			subtotal: 0,
			profile:  TaxProfile{Scheme: TaxSchemeVAT, CountryCode: "DE"},
			wantTax:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tax, rate := calc.Compute(tc.subtotal, tc.profile)
			assert.Equal(t, tc.wantTax, tax)
			if tc.wantRateBPS > 0 {
				assert.Equal(t, tc.wantRateBPS, rate)
			}
		})
	}
}

// Guards default profile inference for EU and US customers.
func TestTaxCalculator_DefaultProfile(t *testing.T) {
	calc := NewTaxCalculator()

	de := calc.DefaultProfile("DE", "EUR")
	assert.Equal(t, TaxSchemeVAT, de.Scheme)
	assert.Equal(t, int32(1900), de.RateBPS)

	us := calc.DefaultProfile("US", "USD")
	assert.Equal(t, TaxSchemeSalesTax, us.Scheme)
	assert.Equal(t, "CA", us.TaxRegion)
}
