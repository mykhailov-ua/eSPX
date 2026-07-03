package billing

import (
	"strings"

	"espx/internal/billing/db"
)

// TaxScheme identifies how tax is applied to an invoice subtotal.
type TaxScheme string

const (
	TaxSchemeNone     TaxScheme = "NONE"
	TaxSchemeVAT      TaxScheme = "VAT"
	TaxSchemeSalesTax TaxScheme = "SALES_TAX"
)

// TaxProfile carries customer metadata used to resolve tax rates.
type TaxProfile struct {
	CountryCode string
	TaxRegion   string
	Scheme      TaxScheme
	RateBPS     int32
}

// TaxCalculator applies VAT or sales tax based on customer metadata.
type TaxCalculator struct{}

// NewTaxCalculator returns a stateless tax resolver for invoice generation.
func NewTaxCalculator() *TaxCalculator {
	return &TaxCalculator{}
}

// ProfileFromDB maps a stored tax profile row into the domain type.
func ProfileFromDB(row db.BillingCustomerTaxProfile) TaxProfile {
	return TaxProfile{
		CountryCode: strings.ToUpper(strings.TrimSpace(row.CountryCode)),
		TaxRegion:   strings.ToUpper(strings.TrimSpace(row.TaxRegion.String)),
		Scheme:      TaxScheme(row.TaxScheme),
		RateBPS:     row.TaxRateBps,
	}
}

// DefaultProfile infers tax scheme from customer country when no profile row exists.
func (calc *TaxCalculator) DefaultProfile(countryCode, currency string) TaxProfile {
	code := strings.ToUpper(strings.TrimSpace(countryCode))
	if code == "" {
		code = "US"
	}
	profile := TaxProfile{CountryCode: code}
	switch {
	case isEUCountry(code):
		profile.Scheme = TaxSchemeVAT
		profile.RateBPS = euVATRateBPS(code)
	case code == "US":
		profile.Scheme = TaxSchemeSalesTax
		profile.TaxRegion = defaultUSRegion(currency)
		profile.RateBPS = usSalesTaxRateBPS(profile.TaxRegion)
	default:
		profile.Scheme = TaxSchemeNone
	}
	return profile
}

// Compute returns tax micro-units for a subtotal using basis-point rates.
func (calc *TaxCalculator) Compute(subtotalMicro int64, profile TaxProfile) (taxMicro int64, rateBPS int32) {
	if subtotalMicro <= 0 || profile.Scheme == TaxSchemeNone {
		return 0, 0
	}
	rateBPS = profile.RateBPS
	if rateBPS <= 0 {
		switch profile.Scheme {
		case TaxSchemeVAT:
			rateBPS = euVATRateBPS(profile.CountryCode)
		case TaxSchemeSalesTax:
			rateBPS = usSalesTaxRateBPS(profile.TaxRegion)
		}
	}
	if rateBPS <= 0 {
		return 0, 0
	}
	taxMicro = (subtotalMicro*int64(rateBPS) + 5000) / 10000
	return taxMicro, rateBPS
}

func defaultUSRegion(currency string) string {
	if strings.EqualFold(currency, "USD") {
		return "CA"
	}
	return ""
}

func usSalesTaxRateBPS(region string) int32 {
	switch strings.ToUpper(region) {
	case "CA":
		return 725
	case "NY":
		return 800
	case "TX":
		return 625
	case "WA":
		return 650
	default:
		return 0
	}
}

func euVATRateBPS(country string) int32 {
	switch strings.ToUpper(country) {
	case "DE":
		return 1900
	case "FR":
		return 2000
	case "IE":
		return 2300
	case "NL":
		return 2100
	case "ES":
		return 2100
	case "IT":
		return 2200
	default:
		return 2000
	}
}

func isEUCountry(code string) bool {
	switch strings.ToUpper(code) {
	case "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
		"DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
		"PL", "PT", "RO", "SK", "SI", "ES", "SE":
		return true
	default:
		return false
	}
}

// MapSchemeToDB converts domain tax scheme to sqlc enum.
func MapSchemeToDB(scheme TaxScheme) db.BillingTaxScheme {
	switch scheme {
	case TaxSchemeVAT:
		return db.BillingTaxSchemeVAT
	case TaxSchemeSalesTax:
		return db.BillingTaxSchemeSALESTAX
	default:
		return db.BillingTaxSchemeNONE
	}
}

// MapSchemeFromDB converts sqlc enum to domain tax scheme.
func MapSchemeFromDB(scheme db.BillingTaxScheme) TaxScheme {
	switch scheme {
	case db.BillingTaxSchemeVAT:
		return TaxSchemeVAT
	case db.BillingTaxSchemeSALESTAX:
		return TaxSchemeSalesTax
	default:
		return TaxSchemeNone
	}
}
