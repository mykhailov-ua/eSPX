package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"time"
)

// SlackProvider delivers messages via incoming webhook URLs.
type SlackProvider struct {
	defaultWebhook string
	breaker        *CircuitBreaker
	client         *http.Client
}

// NewSlackProvider binds a default webhook used when the recipient field is empty.
func NewSlackProvider(defaultWebhook string, breaker *CircuitBreaker) *SlackProvider {
	return &SlackProvider{
		defaultWebhook: defaultWebhook,
		breaker:        breaker,
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
	notificationID, _ := ctx.Value("notification_id").(string)
	ipRegex := regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)

	var buttons []map[string]interface{}

	if notificationID != "" {
		buttons = append(buttons, map[string]interface{}{
			"type": "button",
			"text": map[string]interface{}{
				"type": "plain_text",
				"text": "✅ Acknowledge",
			},
			"url": fmt.Sprintf("https://admin.espx.dev/admin/acknowledge?id=%s", notificationID),
		})
	}

	if ip := ipRegex.FindString(body + " " + title); ip != "" {
		buttons = append(buttons, map[string]interface{}{
			"type":  "button",
			"style": "danger",
			"text": map[string]interface{}{
				"type": "plain_text",
				"text": fmt.Sprintf("🚫 Block IP %s", ip),
			},
			"url": fmt.Sprintf("https://admin.espx.dev/admin/blacklist?ip=%s&source=manual", ip),
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
		respBody, _ := io.ReadAll(resp.Body)
		s.breaker.RecordFailure()
		return fmt.Errorf("slack webhook returned status %d: %s", resp.StatusCode, string(respBody))
	}

	s.breaker.RecordSuccess()
	return nil
}
