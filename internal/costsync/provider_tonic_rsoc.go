package costsync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"espx/pkg/money"
)

// TonicRSOCProvider blends intraday EPC with stats_by_country adjustments (10-day finalization).
type TonicRSOCProvider struct {
	BaseURL string
	Client  *http.Client
}

func (p *TonicRSOCProvider) Network() string { return "tonic_rsoc" }

type tonicEPCResponse struct {
	Data []struct {
		CampaignID string  `json:"campaign_id"`
		Country    string  `json:"country"`
		EPC        float64 `json:"epc"`
		Clicks     int64   `json:"clicks"`
		Revenue    float64 `json:"revenue"`
	} `json:"data"`
}

type tonicStatsResponse struct {
	Data []struct {
		CampaignID string  `json:"campaign_id"`
		Country    string  `json:"country"`
		Revenue    float64 `json:"revenue"`
	} `json:"data"`
}

func (p *TonicRSOCProvider) Fetch(ctx context.Context, cred Credential, date time.Time) ([]CostLine, error) {
	base := p.BaseURL
	if base == "" {
		base = "https://api.tonic.com/v4"
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}

	dateStr := date.Format("2006-01-02")
	useFinal := time.Since(date) >= 10*24*time.Hour

	var lines []CostLine
	if useFinal {
		finalLines, err := p.fetchStatsByCountry(ctx, client, base, cred, dateStr)
		if err != nil {
			return nil, err
		}
		lines = finalLines
	} else {
		epcLines, err := p.fetchEPCDaily(ctx, client, base, cred, dateStr)
		if err != nil {
			return nil, err
		}
		lines = epcLines
	}
	return lines, nil
}

func (p *TonicRSOCProvider) fetchEPCDaily(ctx context.Context, client *http.Client, base string, cred Credential, date string) ([]CostLine, error) {
	endpoint := fmt.Sprintf("%s/epc/daily?date=%s", strings.TrimRight(base, "/"), date)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	p.setAuth(req, cred)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tonic epc/daily: status %d: %s", resp.StatusCode, string(body))
	}

	var parsed tonicEPCResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}

	d, _ := time.Parse("2006-01-02", date)
	lines := make([]CostLine, 0, len(parsed.Data))
	for _, row := range parsed.Data {
		revenueMicro, err := tonicRevenueMicro(row.Revenue, row.EPC, row.Clicks)
		if err != nil || revenueMicro <= 0 {
			continue
		}
		lines = append(lines, CostLine{
			CustomerID:  cred.CustomerID,
			CampaignID:  uuid.NewSHA1(cred.CustomerID, []byte("tonic:"+row.CampaignID)),
			Date:        d,
			Network:     p.Network(),
			PlacementID: row.Country,
			LineType:    LineTypeRevenue,
			AmountMicro: revenueMicro,
			Currency:    "USD",
		})
	}
	return lines, nil
}

func tonicRevenueMicro(revenue, epc float64, clicks int64) (int64, error) {
	if revenue > 0 {
		return money.JSONAmountToMicro(revenue)
	}
	if epc <= 0 || clicks <= 0 {
		return 0, nil
	}
	epcMicro, err := money.JSONAmountToMicro(epc)
	if err != nil {
		return 0, err
	}
	return money.MulMicro(epcMicro, clicks), nil
}

func (p *TonicRSOCProvider) fetchStatsByCountry(ctx context.Context, client *http.Client, base string, cred Credential, date string) ([]CostLine, error) {
	endpoint := fmt.Sprintf("%s/rsoc/stats_by_country?date=%s", strings.TrimRight(base, "/"), date)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	p.setAuth(req, cred)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tonic stats_by_country: status %d: %s", resp.StatusCode, string(body))
	}

	var parsed tonicStatsResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}

	d, _ := time.Parse("2006-01-02", date)
	lines := make([]CostLine, 0, len(parsed.Data))
	for _, row := range parsed.Data {
		revenueMicro, err := money.JSONAmountToMicro(row.Revenue)
		if err != nil || revenueMicro <= 0 {
			continue
		}
		lines = append(lines, CostLine{
			CustomerID:  cred.CustomerID,
			CampaignID:  uuid.NewSHA1(cred.CustomerID, []byte("tonic:"+row.CampaignID)),
			Date:        d,
			Network:     p.Network(),
			PlacementID: row.Country,
			LineType:    LineTypeRevenue,
			AmountMicro: revenueMicro,
			Currency:    "USD",
		})
	}
	return lines, nil
}

func (p *TonicRSOCProvider) setAuth(req *http.Request, cred Credential) {
	if cred.APIKey != "" && cred.ExtraConfig["secret"] != "" {
		req.SetBasicAuth(cred.APIKey, cred.ExtraConfig["secret"])
	} else if cred.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+cred.AccessToken)
	}
}

// System1RSOCProvider fetches hourly intraday revenue with 10-day reconciliation.
type System1RSOCProvider struct {
	BaseURL string
	Client  *http.Client
}

func (p *System1RSOCProvider) Network() string { return "system1_rsoc" }

type system1HourlyResponse struct {
	Rows []struct {
		CampaignID string  `json:"campaign_id"`
		SubID      string  `json:"sub_id"`
		Revenue    float64 `json:"revenue"`
		Final      bool    `json:"final"`
	} `json:"rows"`
}

func (p *System1RSOCProvider) Fetch(ctx context.Context, cred Credential, date time.Time) ([]CostLine, error) {
	base := p.BaseURL
	if base == "" {
		base = "https://api.system1.com/partner/v1"
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}

	useFinal := time.Since(date) >= 10*24*time.Hour
	mode := "hourly"
	if useFinal {
		mode = "final"
	}

	endpoint := fmt.Sprintf("%s/revenue/%s?date=%s", strings.TrimRight(base, "/"), mode, date.Format("2006-01-02"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", cred.APIKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("system1 rsoc: status %d: %s", resp.StatusCode, string(body))
	}

	var parsed system1HourlyResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}

	lines := make([]CostLine, 0, len(parsed.Rows))
	for _, row := range parsed.Rows {
		revenueMicro, err := money.JSONAmountToMicro(row.Revenue)
		if err != nil || revenueMicro <= 0 {
			continue
		}
		lines = append(lines, CostLine{
			CustomerID:  cred.CustomerID,
			CampaignID:  uuid.NewSHA1(cred.CustomerID, []byte("system1:"+row.CampaignID)),
			Date:        date,
			Network:     p.Network(),
			PlacementID: row.SubID,
			LineType:    LineTypeRevenue,
			AmountMicro: revenueMicro,
			Currency:    "USD",
		})
	}
	return lines, nil
}
