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

// TelegramProvider delivers HTML messages via the Telegram Bot API.
type TelegramProvider struct {
	botToken  string
	defaultID string
	breaker   *CircuitBreaker
	client    *http.Client
}

// NewTelegramProvider binds bot credentials and a fallback chat ID for empty recipients.
func NewTelegramProvider(botToken, defaultID string, breaker *CircuitBreaker) *TelegramProvider {
	return &TelegramProvider{
		botToken:  botToken,
		defaultID: defaultID,
		breaker:   breaker,
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

	// 1. Build interactive buttons
	notificationID, _ := ctx.Value("notification_id").(string)
	ipRegex := regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)
	var inlineKeyboard [][]map[string]interface{}

	if notificationID != "" {
		inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
			{
				"text": "✅ Acknowledge Incident",
				"url":  fmt.Sprintf("https://admin.espx.dev/admin/acknowledge?id=%s", notificationID),
			},
		})
	}

	if ip := ipRegex.FindString(body + " " + title); ip != "" {
		inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
			{
				"text": fmt.Sprintf("🚫 Block IP %s", ip),
				"url":  fmt.Sprintf("https://admin.espx.dev/admin/blacklist?ip=%s&source=manual", ip),
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
		respBody, _ := io.ReadAll(resp.Body)
		t.breaker.RecordFailure()
		return fmt.Errorf("telegram api returned status %d: %s", resp.StatusCode, string(respBody))
	}

	t.breaker.RecordSuccess()
	return nil
}
