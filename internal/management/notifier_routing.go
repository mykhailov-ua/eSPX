package management

import (
	"context"
	"fmt"
	"strings"

	"espx/internal/config"
	notifierpb "espx/internal/notifier/pb"
)

type opsAlertTarget struct {
	Provider  notifierpb.Provider
	Recipient string
}

func resolveOpsAlertTarget(cfg *config.Config) (notifierpb.Provider, string, bool) {
	targets := resolveOpsAlertTargets(cfg)
	if len(targets) == 0 {
		return notifierpb.Provider_PROVIDER_UNSPECIFIED, "", false
	}
	primary := targets[0]
	return primary.Provider, primary.Recipient, true
}

func resolveOpsAlertTargets(cfg *config.Config) []opsAlertTarget {
	if cfg == nil {
		return nil
	}

	var targets []opsAlertTarget
	if cfg.Notifier.TelegramChatID != "" {
		targets = append(targets, opsAlertTarget{
			Provider:  notifierpb.Provider_PROVIDER_TELEGRAM,
			Recipient: cfg.Notifier.TelegramChatID,
		})
	}
	if cfg.Notifier.SlackWebhookURL != "" {
		targets = append(targets, opsAlertTarget{
			Provider:  notifierpb.Provider_PROVIDER_SLACK,
			Recipient: string(cfg.Notifier.SlackWebhookURL),
		})
	}
	if cfg.Notifier.SMSDefaultRecipient != "" {
		targets = append(targets, opsAlertTarget{
			Provider:  notifierpb.Provider_PROVIDER_SMS,
			Recipient: cfg.Notifier.SMSDefaultRecipient,
		})
	}
	if cfg.Notifier.SMTPSender != "" {
		targets = append(targets, opsAlertTarget{
			Provider:  notifierpb.Provider_PROVIDER_SMTP,
			Recipient: cfg.Notifier.SMTPSender,
		})
	}
	return targets
}

func resolveBroadcastProviders(cfg *config.Config) []notifierpb.Provider {
	targets := resolveOpsAlertTargets(cfg)
	if len(targets) == 0 {
		return nil
	}
	providers := make([]notifierpb.Provider, 0, len(targets))
	for _, target := range targets {
		providers = append(providers, target.Provider)
	}
	return providers
}

func alertSeverityBroadcast(alert AlertmanagerAlert) bool {
	severity := strings.ToLower(strings.TrimSpace(alert.Labels["severity"]))
	return severity == "critical"
}

func enqueueOpsNotification(
	ctx context.Context,
	client *NotifierClient,
	target opsAlertTarget,
	title, body, dedupKey string,
	broadcast bool,
	broadcastProviders []notifierpb.Provider,
) error {
	if client == nil || client.client == nil {
		return fmt.Errorf("notifier client not configured")
	}

	req := &notifierpb.SendNotificationRequest{
		Provider:  target.Provider,
		Recipient: target.Recipient,
		Title:     title,
		Body:      body,
		DedupKey:  dedupKey,
	}
	if broadcast {
		req.DeliveryMode = notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST
		req.BroadcastProviders = broadcastProviders
	}

	_, err := client.client.SendNotification(ctx, req)
	return err
}

func enqueueOpsNotificationBatch(
	ctx context.Context,
	client *NotifierClient,
	items []opsNotificationItem,
) error {
	if client == nil || client.client == nil {
		return fmt.Errorf("notifier client not configured")
	}
	if len(items) == 0 {
		return nil
	}

	reqs := make([]*notifierpb.SendNotificationRequest, 0, len(items))
	for _, item := range items {
		req := &notifierpb.SendNotificationRequest{
			Provider:  item.Target.Provider,
			Recipient: item.Target.Recipient,
			Title:     item.Title,
			Body:      item.Body,
			DedupKey:  item.DedupKey,
		}
		if item.Broadcast {
			req.DeliveryMode = notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST
			req.BroadcastProviders = item.BroadcastProviders
		}
		reqs = append(reqs, req)
	}

	_, err := client.client.SendNotificationBatch(ctx, &notifierpb.SendNotificationBatchRequest{
		Notifications: reqs,
	})
	return err
}

type opsNotificationItem struct {
	Target             opsAlertTarget
	Title              string
	Body               string
	DedupKey           string
	Broadcast          bool
	BroadcastProviders []notifierpb.Provider
}
