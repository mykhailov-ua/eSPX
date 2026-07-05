package payment

import (
	"context"
	"fmt"

	"espx/internal/config"
	notifierpb "espx/internal/notifier/pb"
)

type opsAlertTarget struct {
	Provider  notifierpb.Provider
	Recipient string
}

func resolveOpsAlertTarget(cfg *config.Config) (notifierpb.Provider, string, bool) {
	if cfg == nil {
		return notifierpb.Provider_PROVIDER_UNSPECIFIED, "", false
	}
	if cfg.Notifier.TelegramChatID != "" {
		return notifierpb.Provider_PROVIDER_TELEGRAM, cfg.Notifier.TelegramChatID, true
	}
	if cfg.Notifier.SlackWebhookURL != "" {
		return notifierpb.Provider_PROVIDER_SLACK, string(cfg.Notifier.SlackWebhookURL), true
	}
	if cfg.Notifier.SMSDefaultRecipient != "" {
		return notifierpb.Provider_PROVIDER_SMS, cfg.Notifier.SMSDefaultRecipient, true
	}
	if cfg.Notifier.SMTPSender != "" {
		return notifierpb.Provider_PROVIDER_SMTP, cfg.Notifier.SMTPSender, true
	}
	return notifierpb.Provider_PROVIDER_UNSPECIFIED, "", false
}

func resolveBroadcastProviders(cfg *config.Config) []notifierpb.Provider {
	if cfg == nil {
		return nil
	}
	var providers []notifierpb.Provider
	if cfg.Notifier.TelegramChatID != "" {
		providers = append(providers, notifierpb.Provider_PROVIDER_TELEGRAM)
	}
	if cfg.Notifier.SlackWebhookURL != "" {
		providers = append(providers, notifierpb.Provider_PROVIDER_SLACK)
	}
	if cfg.Notifier.SMSDefaultRecipient != "" {
		providers = append(providers, notifierpb.Provider_PROVIDER_SMS)
	}
	if cfg.Notifier.SMTPSender != "" {
		providers = append(providers, notifierpb.Provider_PROVIDER_SMTP)
	}
	return providers
}

func enqueueOpsNotification(
	ctx context.Context,
	client *NotifierClient,
	provider notifierpb.Provider,
	recipient, title, body, dedupKey string,
	broadcast bool,
	broadcastProviders []notifierpb.Provider,
) error {
	if client == nil {
		return fmt.Errorf("notifier client not configured")
	}
	req := &notifierpb.SendNotificationRequest{
		Provider:  provider,
		Recipient: recipient,
		Title:     title,
		Body:      body,
		DedupKey:  dedupKey,
	}
	if broadcast {
		req.DeliveryMode = notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST
		req.BroadcastProviders = broadcastProviders
	}
	_, err := client.SendNotification(ctx, req)
	return err
}
