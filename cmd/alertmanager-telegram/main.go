// Command alertmanager-telegram forwards Alertmanager webhook payloads to the Telegram Bot API.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"espx/pkg/lifecycle"
)

type Alert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    time.Time         `json:"startsAt"`
	EndsAt      time.Time         `json:"endsAt"`
}

type AlertmanagerPayload struct {
	Receiver          string            `json:"receiver"`
	Status            string            `json:"status"`
	Alerts            []Alert           `json:"alerts"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	chatID := os.Getenv("TELEGRAM_CHAT_ID")
	port := os.Getenv("PROXY_PORT")
	if port == "" {
		port = "8222"
	}

	if botToken == "" || botToken == "YOUR_TELEGRAM_BOT_TOKEN_PLACEHOLDER" {
		slog.Warn("TELEGRAM_BOT_TOKEN is not configured, running in dry-run mode")
	}
	if chatID == "" || chatID == "YOUR_TELEGRAM_CHAT_ID_PLACEHOLDER" {
		slog.Warn("TELEGRAM_CHAT_ID is not configured, running in dry-run mode")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /alerts", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("failed to read request body", "error", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		var payload AlertmanagerPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			slog.Error("failed to decode alertmanager payload", "error", err)
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}

		slog.Info("received alerts from alertmanager", "count", len(payload.Alerts), "status", payload.Status)

		for _, alert := range payload.Alerts {
			var message string
			statusText := "ALERT ACTIVE"
			if alert.Status == "resolved" {
				statusText = "ALERT RESOLVED"
			}

			severity := alert.Labels["severity"]
			if severity == "" {
				severity = "warning"
			}

			message = fmt.Sprintf(
				"<b>%s</b>\n\n<b>Alert:</b> %s\n<b>Severity:</b> <code>%s</code>\n<b>Description:</b> %s\n<b>Time:</b> <code>%s</code>\n",
				statusText,
				alert.Annotations["summary"],
				severity,
				alert.Annotations["description"],
				alert.StartsAt.In(time.UTC).Format("15:04:05 02-01-2006 UTC"),
			)

			if botToken == "" || chatID == "" || botToken == "YOUR_TELEGRAM_BOT_TOKEN_PLACEHOLDER" || chatID == "YOUR_TELEGRAM_CHAT_ID_PLACEHOLDER" {
				slog.Info("DRY-RUN: Telegram Alert Notification", "message", message)
				continue
			}

			if err := sendTelegramMessage(botToken, chatID, message); err != nil {
				slog.Error("failed to send telegram notification", "error", err)
			}
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("starting alertmanager telegram proxy", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("proxy server failed", "error", err)
			os.Exit(1)
		}
	}()

	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal", "signal", sig.String())

	timeouts := lifecycle.TimeoutsFromEnv()
	if err := lifecycle.ShutdownHTTPServer(server, timeouts.Shutdown); err != nil {
		slog.Error("proxy shutdown failed", "error", err)
	}
	slog.Info("proxy shutdown complete")
}

func sendTelegramMessage(token, chatID, htmlMessage string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
	payload := map[string]interface{}{
		"chat_id":    chatID,
		"text":       htmlMessage,
		"parse_mode": "HTML",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("telegram api returned status %d (failed to read response body: %w)", resp.StatusCode, err)
		}
		return fmt.Errorf("telegram api returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
