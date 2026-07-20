package costsync

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	db "espx/internal/ingestion/sqlc"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const microUnit = int64(1_000_000)

// CurrencyConverter converts foreign amounts to USD micro-units using ECB daily rates.
type CurrencyConverter struct {
	pool   *pgxpool.Pool
	client *http.Client
}

// NewCurrencyConverter constructs an ECB-backed converter with optional PG rate cache.
func NewCurrencyConverter(pool *pgxpool.Pool, client *http.Client) *CurrencyConverter {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &CurrencyConverter{pool: pool, client: client}
}

type ecbDailyRates struct {
	XMLName xml.Name `xml:"Envelope"`
	Cube    struct {
		Cube struct {
			Time string `xml:"time,attr"`
			Cube []struct {
				Currency string  `xml:"currency,attr"`
				Rate     float64 `xml:"rate,attr"`
			} `xml:"Cube"`
		} `xml:"Cube"`
	} `xml:"Cube"`
}

// ToUSDMicro converts amountMicro in currency to USD micro-units for rateDate.
func (c *CurrencyConverter) ToUSDMicro(ctx context.Context, amountMicro int64, currency string, rateDate time.Time) (int64, error) {
	cur := strings.ToUpper(strings.TrimSpace(currency))
	if cur == "" || cur == "USD" {
		return amountMicro, nil
	}

	usdPerUnit, err := c.usdPerUnitMicro(ctx, cur, rateDate)
	if err != nil {
		return 0, err
	}

	// amount_micro is in currency micro-units; usd_per_unit_micro is USD per 1 unit of currency.
	converted := (amountMicro * usdPerUnit) / microUnit
	if converted < 0 {
		return converted, nil
	}
	return converted, nil
}

func (c *CurrencyConverter) usdPerUnitMicro(ctx context.Context, currency string, rateDate time.Time) (int64, error) {
	if c.pool != nil {
		q := db.New(c.pool)
		rate, err := q.GetECBRate(ctx, db.GetECBRateParams{
			RateDate: pgtype.Date{Time: rateDate, Valid: true},
			Currency: currency,
		})
		if err == nil && rate > 0 {
			return rate, nil
		}
	}

	rates, err := c.fetchECBRates(ctx)
	if err != nil {
		return 0, err
	}

	eurPerUSD, ok := rates["USD"]
	if !ok || eurPerUSD <= 0 {
		return 0, fmt.Errorf("ecb: missing USD rate")
	}

	if currency == "EUR" {
		// 1 EUR = (1/eurPerUSD) USD
		usdPerEUR := 1.0 / eurPerUSD
		return floatToMicro(usdPerEUR), nil
	}

	eurPerUnit, ok := rates[currency]
	if !ok || eurPerUnit <= 0 {
		return 0, fmt.Errorf("ecb: unknown currency %s", currency)
	}

	// 1 unit = (eurPerUnit/eurPerUSD) USD
	usdPerUnit := eurPerUnit / eurPerUSD
	micro := floatToMicro(usdPerUnit)

	if c.pool != nil {
		q := db.New(c.pool)
		_ = q.UpsertECBRate(ctx, db.UpsertECBRateParams{
			RateDate:        pgtype.Date{Time: rateDate, Valid: true},
			Currency:        currency,
			UsdPerUnitMicro: micro,
		})
	}
	return micro, nil
}

func (c *CurrencyConverter) fetchECBRates(ctx context.Context) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ecb: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	var doc ecbDailyRates
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil, err
	}

	out := make(map[string]float64, len(doc.Cube.Cube.Cube)+1)
	out["EUR"] = 1.0
	for _, row := range doc.Cube.Cube.Cube {
		out[row.Currency] = row.Rate
	}
	return out, nil
}

func floatToMicro(v float64) int64 {
	return int64(math.Round(v * float64(microUnit)))
}

// ConvertEURToUSD is a test helper using a fixed ECB sample rate (1 EUR = 1.10 USD).
func ConvertEURToUSD(amountMicro int64) int64 {
	return (amountMicro * 1_100_000) / microUnit
}
