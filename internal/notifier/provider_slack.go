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

// SlackProvider delivers messages via incoming webhook URLs.
type SlackProvider struct {
	defaultWebhook     string
	breaker            *CircuitBreaker
	requireCredentials bool
	client             *http.Client
}

// NewSlackProvider binds a default webhook used when the recipient field is empty.
func NewSlackProvider(defaultWebhook string, breaker *CircuitBreaker, requireCredentials bool) *SlackProvider {
	return &SlackProvider{
		defaultWebhook:     defaultWebhook,
		breaker:            breaker,
		requireCredentials: requireCredentials,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *SlackProvider) Name() string {
	return "SLACK"
}

// Send posts to the webhook; missing credentials log a dry-run and return nil.
func (s *SlackProvider) Send(ctx context.Context, recipient, title, body string) error {
	if !s.breaker.Allow() {
		return ErrCircuitOpen
	}

	webhookURL := recipient
	if webhookURL == "" {
		webhookURL = s.defaultWebhook
	}

	if webhookURL == "" {
		if s.requireCredentials {
			return fmt.Errorf("slack webhook not configured")
		}
		slog.Info("slack notification dry-run", "title", title, "body", body)
		return nil
	}

	var text string
	if title != "" {
		text = fmt.Sprintf("*%s*\n%s", title, body)
	} else {
		text = body
	}

	payload := map[string]interface{}{}

	// Build interactive blocks
	notificationID, _ := NotificationIDFromContext(ctx)
	actions := BuildInteractiveActions(notificationID, title, body)
	var buttons []map[string]interface{}

	if actions.AcknowledgeURL != "" {
		buttons = append(buttons, map[string]interface{}{
			"type": "button",
			"text": map[string]interface{}{
				"type": "plain_text",
				"text": "✅ Acknowledge",
			},
			"url": actions.AcknowledgeURL,
		})
	}

	if actions.BlockIPURL != "" {
		buttons = append(buttons, map[string]interface{}{
			"type":  "button",
			"style": "danger",
			"text": map[string]interface{}{
				"type": "plain_text",
				"text": fmt.Sprintf("🚫 Block IP %s", actions.BlockIP),
			},
			"url": actions.BlockIPURL,
		})
	}

	if len(buttons) > 0 {
		payload["blocks"] = []interface{}{
			map[string]interface{}{
				"type": "section",
				"text": map[string]interface{}{
					"type": "mrkdwn",
					"text": text,
				},
			},
			map[string]interface{}{
				"type":     "actions",
				"elements": buttons,
			},
		}
	} else {
		payload["text"] = text
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		s.breaker.RecordFailure()
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		s.breaker.RecordFailure()
		if readErr != nil {
			return fmt.Errorf("slack webhook returned status %d: read body: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("slack webhook returned status %d: %s", resp.StatusCode, string(respBody))
	}

	s.breaker.RecordSuccess()
	return nil
}
