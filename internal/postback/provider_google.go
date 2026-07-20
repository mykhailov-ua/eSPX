package postback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type GoogleAdapter struct{}

type GoogleOfflineConversion struct {
	Gclid            string  `json:"gclid"`
	ConversionValue  float64 `json:"conversion_value"`
	CurrencyCode     string  `json:"currency_code"`
	ConversionTime   string  `json:"conversion_time"`
	ConversionAction string  `json:"conversion_action"`
}

type GoogleCAPIPayload struct {
	Conversions []GoogleOfflineConversion `json:"conversions"`
}

func (a *GoogleAdapter) Send(ctx context.Context, client *http.Client, payload *PostbackPayload, urlTemplate string, apiTokenDecrypted string) error {
	url := urlTemplate
	if url == "" || !strings.HasPrefix(url, "http") {
		// Default mock google ads URL
		url = "https://googleads.googleapis.com/v15/customers/default/offlineUserDataJobs:run"
	}

	action := "Conversion"
	if payload.EventType != "" {
		action = payload.EventType
	}

	conv := GoogleOfflineConversion{
		Gclid:            payload.GCLID,
		ConversionValue:  payload.Payout,
		CurrencyCode:     "USD",
		ConversionTime:   time.Now().UTC().Format("2006-01-02 15:04:05-07:00"),
		ConversionAction: action,
	}

	googlePayload := GoogleCAPIPayload{
		Conversions: []GoogleOfflineConversion{conv},
	}

	bodyBytes, err := json.Marshal(googlePayload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if apiTokenDecrypted != "" {
		req.Header.Set("Authorization", "Bearer "+apiTokenDecrypted)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
