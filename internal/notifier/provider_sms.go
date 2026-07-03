package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// SMSProvider delivers SMS alerts via an external HTTP API (e.g., Twilio or SMSC).
type SMSProvider struct {
	providerURL      string
	apiToken         string
	defaultRecipient string
	breaker          *CircuitBreaker
	client           *http.Client
}

// NewSMSProvider binds SMS API credentials and default fallback recipient.
func NewSMSProvider(providerURL, apiToken, defaultRecipient string, breaker *CircuitBreaker) *SMSProvider {
	return &SMSProvider{
		providerURL:      providerURL,
		apiToken:         apiToken,
		defaultRecipient: defaultRecipient,
		breaker:          breaker,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *SMSProvider) Name() string {
	return "SMS"
}

// Send delivers SMS; missing credentials trigger a dry-run log and return nil.
func (s *SMSProvider) Send(ctx context.Context, recipient, title, body string) error {
	if !s.breaker.Allow() {
		return ErrCircuitOpen
	}

	phone := recipient
	if phone == "" {
		phone = s.defaultRecipient
	}

	if s.providerURL == "" || phone == "" {
		slog.Info("sms notification dry-run", "to", phone, "title", title, "body", body)
		return nil
	}

	// Format text message
	var text string
	if title != "" {
		text = fmt.Sprintf("[%s] %s", title, body)
	} else {
		text = body
	}

	payload := map[string]interface{}{
		"to":      phone,
		"message": text,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.providerURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.apiToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.apiToken))
	}

	resp, err := s.client.Do(req)
	if err != nil {
		s.breaker.RecordFailure()
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		s.breaker.RecordFailure()
		return fmt.Errorf("sms api returned status %d: %s", resp.StatusCode, string(respBody))
	}

	s.breaker.RecordSuccess()
	return nil
}
