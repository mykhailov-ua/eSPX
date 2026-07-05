package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// TelegramProvider delivers HTML messages via the Telegram Bot API.
type TelegramProvider struct {
	botToken           string
	defaultID          string
	breaker            *CircuitBreaker
	requireCredentials bool
	client             *http.Client
}

// NewTelegramProvider binds bot credentials and a fallback chat ID for empty recipients.
func NewTelegramProvider(botToken, defaultID string, breaker *CircuitBreaker, requireCredentials bool) *TelegramProvider {
	return &TelegramProvider{
		botToken:           botToken,
		defaultID:          defaultID,
		breaker:            breaker,
		requireCredentials: requireCredentials,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (t *TelegramProvider) Name() string {
	return "TELEGRAM"
}

// Send delivers via sendMessage; missing credentials log a dry-run and return nil.
func (t *TelegramProvider) Send(ctx context.Context, recipient, title, body string) error {
	if !t.breaker.Allow() {
		return ErrCircuitOpen
	}

	chatID := recipient
	if chatID == "" {
		chatID = t.defaultID
	}

	if t.botToken == "" || chatID == "" {
		if t.requireCredentials {
			return fmt.Errorf("telegram credentials not configured")
		}
		slog.Info("telegram notification dry-run", "title", title, "body", body)
		return nil
	}

	var htmlMessage string
	if title != "" {
		htmlMessage = fmt.Sprintf("<b>%s</b>\n\n%s", title, body)
	} else {
		htmlMessage = body
	}

	payload := map[string]interface{}{
		"chat_id":    chatID,
		"text":       htmlMessage,
		"parse_mode": "HTML",
	}

	// Build interactive buttons
	notificationID, _ := NotificationIDFromContext(ctx)
	actions := BuildInteractiveActions(notificationID, title, body)
	var inlineKeyboard [][]map[string]interface{}

	if actions.AcknowledgeURL != "" {
		inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
			{
				"text": "✅ Acknowledge Incident",
				"url":  actions.AcknowledgeURL,
			},
		})
	}

	if actions.BlockIPURL != "" {
		inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
			{
				"text": fmt.Sprintf("🚫 Block IP %s", actions.BlockIP),
				"url":  actions.BlockIPURL,
			},
		})
	}

	if len(inlineKeyboard) > 0 {
		payload["reply_markup"] = map[string]interface{}{
			"inline_keyboard": inlineKeyboard,
		}
	}

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.botToken)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		t.breaker.RecordFailure()
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter := parseTelegramRetryAfter(resp, respBody)
			slog.Warn("telegram api rate limited", "retry_after", retryAfter)
			return &ProviderRateLimitedError{Provider: "TELEGRAM", RetryAfter: retryAfter}
		}
		t.breaker.RecordFailure()
		if readErr != nil {
			return fmt.Errorf("telegram api returned status %d: read body: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("telegram api returned status %d: %s", resp.StatusCode, string(respBody))
	}

	t.breaker.RecordSuccess()
	return nil
}

func parseTelegramRetryAfter(resp *http.Response, body []byte) time.Duration {
	if resp != nil {
		if header := resp.Header.Get("Retry-After"); header != "" {
			if sec, err := strconv.Atoi(header); err == nil && sec > 0 {
				return time.Duration(sec) * time.Second
			}
		}
	}
	var apiResp struct {
		Parameters struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if len(body) > 0 && json.Unmarshal(body, &apiResp) == nil && apiResp.Parameters.RetryAfter > 0 {
		return time.Duration(apiResp.Parameters.RetryAfter) * time.Second
	}
	return 30 * time.Second
}
