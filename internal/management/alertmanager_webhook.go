package management

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"espx/internal/config"
	"espx/internal/metrics"
	notifierpb "espx/internal/notifier/pb"
	"espx/pkg/coldpath"
)

// AlertmanagerAlert mirrors one Alertmanager notification in webhook JSON.
type AlertmanagerAlert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    time.Time         `json:"startsAt"`
	EndsAt      time.Time         `json:"endsAt"`
}

// AlertmanagerPayload is the webhook envelope Alertmanager posts to receivers.
type AlertmanagerPayload struct {
	Receiver          string              `json:"receiver"`
	Status            string              `json:"status"`
	Alerts            []AlertmanagerAlert `json:"alerts"`
	GroupLabels       map[string]string   `json:"groupLabels"`
	CommonLabels      map[string]string   `json:"commonLabels"`
	CommonAnnotations map[string]string   `json:"commonAnnotations"`
	ExternalURL       string              `json:"externalURL"`
}

// AlertmanagerWebhook forwards Prometheus alerts to the notifier gRPC service.
type AlertmanagerWebhook struct {
	client             *NotifierClient
	provider           notifierpb.Provider
	recipient          string
	broadcastProviders []notifierpb.Provider
	token              string
	dryRun             bool
}

// NewAlertmanagerWebhook constructs a webhook handler when alert routing is configured.
func NewAlertmanagerWebhook(client *NotifierClient, cfg *config.Config) *AlertmanagerWebhook {
	if client == nil || cfg == nil || !cfg.AlertmanagerWebhookEnabled() {
		return nil
	}
	provider, recipient, ok := resolveOpsAlertTarget(cfg)
	if !ok {
		return nil
	}
	return &AlertmanagerWebhook{
		client:             client,
		provider:           provider,
		recipient:          recipient,
		broadcastProviders: resolveBroadcastProviders(cfg),
		token:              cfg.Management.AlertmanagerWebhookToken,
		dryRun:             cfg.Env != "production" && cfg.Env != "prod" && !cfg.NotifierConfigured(),
	}
}

// Register mounts POST /ops/alertmanager/webhook when the adapter is enabled.
func (h *AlertmanagerWebhook) Register(mux *http.ServeMux) {
	if h == nil {
		return
	}
	mux.HandleFunc("POST /ops/alertmanager/webhook", h.handle)
}

func (h *AlertmanagerWebhook) handle(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		http.NotFound(w, r)
		return
	}
	if h.token != "" && r.Header.Get("X-Alertmanager-Token") != h.token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var payload AlertmanagerPayload
	if err := coldpath.UnmarshalJSON(body, &payload); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	slog.Info("alertmanager webhook received",
		"count", len(payload.Alerts),
		"status", payload.Status,
	)

	var batchItems []opsNotificationItem
	for _, alert := range payload.Alerts {
		title, message := FormatAlertmanagerAlert(alert)
		if h.dryRun {
			slog.Info("alertmanager webhook dry-run", "title", title, "message", message)
			continue
		}
		batchItems = append(batchItems, opsNotificationItem{
			Target:             opsAlertTarget{Provider: h.provider, Recipient: h.recipient},
			Title:              title,
			Body:               message,
			DedupKey:           alertmanagerDedupKey(alert),
			Broadcast:          alertSeverityBroadcast(alert),
			BroadcastProviders: h.broadcastProviders,
		})
	}

	if len(batchItems) > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		sendErr := enqueueOpsNotificationBatch(ctx, h.client, batchItems)
		cancel()
		if sendErr != nil {
			metrics.ManagementOpsAlertEnqueueFailuresTotal.Add(float64(len(batchItems)))
			slog.Warn("alertmanager webhook batch enqueue failed", "error", sendErr, "count", len(batchItems))
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// FormatAlertmanagerAlert renders one alert as HTML suitable for Telegram/Slack providers.
func FormatAlertmanagerAlert(alert AlertmanagerAlert) (title, body string) {
	statusText := "ALERT ACTIVE"
	if alert.Status == "resolved" {
		statusText = "ALERT RESOLVED"
	}

	severity := alert.Labels["severity"]
	if severity == "" {
		severity = "warning"
	}
	alertName := alert.Labels["alertname"]
	if alertName == "" {
		alertName = alert.Annotations["summary"]
	}
	if alertName == "" {
		alertName = "unknown"
	}

	summary := alert.Annotations["summary"]
	if summary == "" {
		summary = alertName
	}
	description := alert.Annotations["description"]
	if description == "" {
		description = "—"
	}

	title = fmt.Sprintf("eSPX: %s", alertName)
	body = fmt.Sprintf(
		"<b>%s</b>\n\n<b>Alert:</b> %s\n<b>Severity:</b> <code>%s</code>\n<b>Description:</b> %s\n<b>Time:</b> <code>%s</code>",
		statusText,
		summary,
		severity,
		description,
		alert.StartsAt.In(time.UTC).Format("15:04:05 02-01-2006 UTC"),
	)
	return title, body
}

func alertmanagerDedupKey(alert AlertmanagerAlert) string {
	alertName := alert.Labels["alertname"]
	if alertName == "" {
		alertName = alert.Annotations["summary"]
	}
	if alertName == "" {
		alertName = "unknown"
	}
	return fmt.Sprintf("alertmanager:%s:%s", alertName, alert.Status)
}

// FormatAlertmanagerAlertText exposes the rendered body for tests.
func FormatAlertmanagerAlertText(alert AlertmanagerAlert) string {
	_, body := FormatAlertmanagerAlert(alert)
	return body
}

// AlertmanagerDryRun reports whether notifications are logged instead of enqueued.
func (h *AlertmanagerWebhook) AlertmanagerDryRun() bool {
	if h == nil {
		return true
	}
	return h.dryRun
}

// AlertmanagerRecipient exposes the configured notifier recipient for tests.
func (h *AlertmanagerWebhook) AlertmanagerRecipient() string {
	if h == nil {
		return ""
	}
	return strings.TrimSpace(h.recipient)
}
