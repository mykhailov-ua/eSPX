package postback

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type FacebookAdapter struct{}

type FacebookEvent struct {
	EventName string             `json:"event_name"`
	EventTime int64              `json:"event_time"`
	UserData  FacebookUserData   `json:"user_data"`
	CustomData FacebookCustomData `json:"custom_data,omitempty"`
}

type FacebookUserData struct {
	Em  []string `json:"em,omitempty"`
	Ph  []string `json:"ph,omitempty"`
	Fbc string   `json:"fbc,omitempty"`
}

type FacebookCustomData struct {
	Value    float64 `json:"value,omitempty"`
	Currency string  `json:"currency,omitempty"`
}

type FacebookCAPIPayload struct {
	Data []FacebookEvent `json:"data"`
}

func hashSHA256(input string) string {
	if input == "" {
		return ""
	}
	h := sha256.New()
	h.Write([]byte(strings.TrimSpace(strings.ToLower(input))))
	return hex.EncodeToString(h.Sum(nil))
}

func (a *FacebookAdapter) Send(ctx context.Context, client *http.Client, payload *PostbackPayload, urlTemplate string, apiTokenDecrypted string) error {
	// Standard FB CAPI endpoint: https://graph.facebook.com/v19.0/{pixel_id}/events
	// If urlTemplate is a full URL (e.g. for testing), we use it. Otherwise we construct it.
	url := urlTemplate
	if url == "" || !strings.HasPrefix(url, "http") {
		pixelID := urlTemplate
		if pixelID == "" {
			pixelID = "default_pixel"
		}
		url = fmt.Sprintf("https://graph.facebook.com/v19.0/%s/events", pixelID)
	}

	eventName := "Lead"
	if strings.ToLower(payload.EventType) == "conversion" || strings.ToLower(payload.EventType) == "purchase" {
		eventName = "Purchase"
	}

	// PII egress: hash email/phone SHA-256 before FB CAPI
	var ems []string
	if payload.Email != "" {
		ems = []string{hashSHA256(payload.Email)}
	}
	var phs []string
	if payload.Phone != "" {
		phs = []string{hashSHA256(payload.Phone)}
	}

	fbc := payload.FBCLID
	if fbc != "" && !strings.HasPrefix(fbc, "fb.1.") {
		// format as standard fbc: fb.1.epoch.click_id
		fbc = fmt.Sprintf("fb.1.%d.%s", time.Now().Unix(), fbc)
	}

	fbEvent := FacebookEvent{
		EventName: eventName,
		EventTime: time.Now().Unix(),
		UserData: FacebookUserData{
			Em:  ems,
			Ph:  phs,
			Fbc: fbc,
		},
	}

	if payload.Payout > 0 {
		fbEvent.CustomData = FacebookCustomData{
			Value:    payload.Payout,
			Currency: "USD",
		}
	}

	capiPayload := FacebookCAPIPayload{
		Data: []FacebookEvent{fbEvent},
	}

	bodyBytes, err := json.Marshal(capiPayload)
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
