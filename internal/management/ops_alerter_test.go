package management

import (
	"strings"
	"testing"
	"time"

	"espx/internal/config"
	notifierpb "espx/internal/notifier/pb"
)

func TestResolveOpsAlertTarget_TelegramPreferred(t *testing.T) {
	cfg := &config.Config{}
	cfg.Notifier.TelegramChatID = "-100123"
	cfg.Notifier.SlackWebhookURL = "https://hooks.slack.com/test"

	provider, recipient, ok := resolveOpsAlertTarget(cfg)
	if !ok {
		t.Fatal("expected target")
	}
	if provider != notifierpb.Provider_PROVIDER_TELEGRAM {
		t.Fatalf("provider: got %v want TELEGRAM", provider)
	}
	if recipient != "-100123" {
		t.Fatalf("recipient: got %q", recipient)
	}
}

func TestOpsAlerter_CooldownDedup(t *testing.T) {
	cfg := &config.Config{}
	cfg.Management.OpsAlertsEnabled = true
	cfg.Management.OpsAlertCooldownSec = 60
	cfg.Notifier.TelegramChatID = "-100123"

	alerter := NewOpsAlerter(&NotifierClient{}, cfg)
	if alerter == nil {
		t.Fatal("expected alerter")
	}

	if !alerter.shouldSend("redis:shard:0") {
		t.Fatal("first send should pass")
	}
	if alerter.shouldSend("redis:shard:0") {
		t.Fatal("second send within cooldown should be suppressed")
	}

	alerter.lastSent.Store("redis:shard:0", time.Now().Add(-2*time.Minute))
	if !alerter.shouldSend("redis:shard:0") {
		t.Fatal("send after cooldown should pass")
	}
}

func TestNewOpsAlerter_DisabledWithoutRecipient(t *testing.T) {
	cfg := &config.Config{}
	cfg.Management.OpsAlertsEnabled = true

	if NewOpsAlerter(&NotifierClient{}, cfg) != nil {
		t.Fatal("expected nil without recipient")
	}
}

func TestFormatSlotIDs_TruncatesLongLists(t *testing.T) {
	slots := make([]int16, 20)
	for i := range slots {
		slots[i] = int16(i)
	}
	got := formatSlotIDs(slots)
	if !strings.Contains(got, "+8 more") {
		t.Fatalf("expected truncation, got %q", got)
	}
}

func TestNewNotifierClient_DisabledByDefault(t *testing.T) {
	client, err := NewNotifierClient(&config.Config{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != nil {
		t.Fatal("expected nil client when ops alerts disabled")
	}
}
