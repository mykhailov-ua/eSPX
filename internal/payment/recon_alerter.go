package payment

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"espx/internal/config"
	"espx/internal/metrics"
	notifierpb "espx/internal/notifier/pb"
)

// FinancialReconAlerter notifies operators when financial recon surfaces WARN+ findings.
type FinancialReconAlerter struct {
	client             *NotifierClient
	provider           notifierpb.Provider
	recipient          string
	broadcastProviders []notifierpb.Provider
	cooldown           time.Duration
	lastSent           sync.Map // alert key -> time.Time
}

// NewFinancialReconAlerter constructs an alerter when OPS_ALERTS_ENABLED and a recipient are set.
func NewFinancialReconAlerter(client *NotifierClient, cfg *config.Config) *FinancialReconAlerter {
	if client == nil || cfg == nil || !cfg.OpsAlertsEnabled() {
		return nil
	}
	provider, recipient, ok := resolveOpsAlertTarget(cfg)
	if !ok {
		return nil
	}
	cooldownSec := cfg.Management.OpsAlertCooldownSec
	if cooldownSec <= 0 {
		cooldownSec = 300
	}
	return &FinancialReconAlerter{
		client:             client,
		provider:           provider,
		recipient:          recipient,
		broadcastProviders: resolveBroadcastProviders(cfg),
		cooldown:           time.Duration(cooldownSec) * time.Second,
	}
}

func (a *FinancialReconAlerter) shouldSend(key string) bool {
	if a == nil {
		return false
	}
	now := time.Now()
	if v, ok := a.lastSent.Load(key); ok {
		if now.Sub(v.(time.Time)) < a.cooldown {
			return false
		}
	}
	a.lastSent.Store(key, now)
	return true
}

// AlertFindings enqueues a notifier message when any finding severity is WARN or higher.
func (a *FinancialReconAlerter) AlertFindings(summary FinancialReconSummary, findings []FinancialReconFinding) {
	if a == nil || len(findings) == 0 {
		return
	}

	var alertable []FinancialReconFinding
	hasCritical := false
	for _, f := range findings {
		sev := financialFindingSeverity(f.Kind)
		if !severityAtLeastWarn(sev) {
			continue
		}
		alertable = append(alertable, f)
		if sev >= SeverityCritical {
			hasCritical = true
		}
	}
	if len(alertable) == 0 {
		return
	}

	key := fmt.Sprintf("payment-financial-recon:run:%d", summary.RunID)
	if !a.shouldSend(key) {
		return
	}

	title := "eSPX: payment financial recon findings"
	body := formatFinancialReconAlertBody(summary, alertable)

	a.sendAsync(key, title, body, hasCritical)
}

func (a *FinancialReconAlerter) sendAsync(key, title, body string, broadcast bool) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := enqueueOpsNotification(ctx, a.client, a.provider, a.recipient, title, body, key, broadcast, a.broadcastProviders); err != nil {
			metrics.ManagementOpsAlertEnqueueFailuresTotal.Inc()
			slog.Warn("payment financial recon alert enqueue failed", "key", key, "error", err)
		}
	}()
}

func formatFinancialReconAlertBody(summary FinancialReconSummary, findings []FinancialReconFinding) string {
	var b strings.Builder
	b.WriteString("<b>Payment financial recon</b>\n")
	b.WriteString(fmt.Sprintf("Run #%d\nPeriod: %s — %s\n",
		summary.RunID,
		summary.PeriodStart.UTC().Format(time.RFC3339),
		summary.PeriodEnd.UTC().Format(time.RFC3339),
	))
	b.WriteString(fmt.Sprintf("Intents checked: %d\nAlertable findings: %d\n\n",
		summary.IntentsChecked, len(findings)))

	counts := make(map[string]int)
	for _, f := range findings {
		counts[string(f.Kind)]++
	}
	for kind, n := range counts {
		b.WriteString(fmt.Sprintf("• %s: %d\n", kind, n))
	}
	return b.String()
}
