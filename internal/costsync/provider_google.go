package costsync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// GoogleAdsProvider fetches spend via Google Ads API searchStream (simplified REST shim).
type GoogleAdsProvider struct {
	BaseURL string
	Client  *http.Client
}

func (p *GoogleAdsProvider) Network() string { return "google" }

type googleAdsReportResponse struct {
	Results []struct {
		Campaign struct {
			ID string `json:"id"`
		} `json:"campaign"`
		AdGroup struct {
			ID string `json:"id"`
		} `json:"adGroup"`
		Metrics struct {
			CostMicros string `json:"costMicros"`
		} `json:"metrics"`
		Segments struct {
			Date string `json:"date"`
		} `json:"segments"`
	} `json:"results"`
}

func (p *GoogleAdsProvider) Fetch(ctx context.Context, cred Credential, date time.Time) ([]CostLine, error) {
	base := p.BaseURL
	if base == "" {
		base = "https://googleads.googleapis.com/v16"
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}

	customerID := cred.AccountID
	if customerID == "" {
		customerID = cred.ExtraConfig["customer_id"]
	}
	if customerID == "" {
		return nil, fmt.Errorf("google ads: missing customer id")
	}

	query := fmt.Sprintf(`SELECT campaign.id, ad_group.id, metrics.cost_micros, segments.date FROM ad_group WHERE segments.date = '%s'`, date.Format("2006-01-02"))
	payload := map[string]string{"query": query}
	bodyBytes, _ := json.Marshal(payload)

	endpoint := fmt.Sprintf("%s/customers/%s/googleAds:searchStream", strings.TrimRight(base, "/"), customerID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cred.AccessToken)
	if devToken := cred.ExtraConfig["developer_token"]; devToken != "" {
		req.Header.Set("developer-token", devToken)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("google ads: status %d: %s", resp.StatusCode, string(respBody))
	}

	var parsed googleAdsReportResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, err
	}

	lines := make([]CostLine, 0, len(parsed.Results))
	for _, row := range parsed.Results {
		var costMicro int64
		costMicro, err = strconv.ParseInt(row.Metrics.CostMicros, 10, 64)
		if err != nil || costMicro == 0 {
			continue
		}
		lines = append(lines, CostLine{
			CustomerID:  cred.CustomerID,
			CampaignID:  uuid.NewSHA1(cred.CustomerID, []byte("google:"+row.Campaign.ID)),
			Date:        date,
			Network:     p.Network(),
			PlacementID: row.AdGroup.ID,
			AdsetID:     row.AdGroup.ID,
			LineType:    LineTypeSpend,
			AmountMicro: costMicro,
			Currency:    "USD",
		})
	}
	return lines, nil
}
