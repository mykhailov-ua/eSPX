package management

import (
	"strings"
	"testing"
	"time"

	"espx/internal/config"
)

func TestFormatAlertmanagerAlert_Active(t *testing.T) {
	start := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	_, body := FormatAlertmanagerAlert(AlertmanagerAlert{
		Status: "firing",
		Labels: map[string]string{
			"alertname": "HighErrorRate",
			"severity":  "critical",
		},
		Annotations: map[string]string{
			"summary":     "Tracker errors elevated",
			"description": "5xx ratio above SLO",
		},
		StartsAt: start,
	})

	for _, want := range []string{"ALERT ACTIVE", "critical", "Tracker errors elevated", "5xx ratio above SLO"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q: %s", want, body)
		}
	}
}

func TestFormatAlertmanagerAlert_Resolved(t *testing.T) {
	_, body := FormatAlertmanagerAlert(AlertmanagerAlert{
		Status: "resolved",
		Labels: map[string]string{"severity": "warning"},
		Annotations: map[string]string{
			"summary": "Recovered",
		},
		StartsAt: time.Now().UTC(),
	})
	if !strings.Contains(body, "ALERT RESOLVED") {
		t.Fatalf("expected resolved marker in %q", body)
	}
}

func TestNewAlertmanagerWebhook_DisabledByDefault(t *testing.T) {
	cfg := &config.Config{}
	cfg.Management.AlertmanagerWebhookEnabled = false
	if NewAlertmanagerWebhook(&NotifierClient{}, cfg) != nil {
		t.Fatal("expected nil when disabled")
	}
}

func TestNewAlertmanagerWebhook_EnabledWithRecipient(t *testing.T) {
	cfg := &config.Config{}
	cfg.Management.AlertmanagerWebhookEnabled = true
	cfg.Notifier.TelegramChatID = "-100123"
	h := NewAlertmanagerWebhook(&NotifierClient{}, cfg)
	if h == nil {
		t.Fatal("expected handler")
	}
	if h.AlertmanagerRecipient() != "-100123" {
		t.Fatalf("recipient: got %q", h.AlertmanagerRecipient())
	}
}
