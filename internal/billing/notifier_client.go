package billing

import (
	"fmt"

	"espx/internal/config"
	notifierpb "espx/internal/notifier/pb"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewNotifierClient dials notifier when invoice delivery or drift alerts are enabled.
func NewNotifierClient(cfg *config.Config) (notifierpb.NotifierServiceClient, func() error, error) {
	if cfg == nil {
		return nil, func() error { return nil }, nil
	}
	_, recipient := ResolveInvoiceNotifierTarget(cfg)
	if recipient == "" {
		return nil, func() error { return nil }, nil
	}
	target := cfg.Notifier.ServerHost + ":" + cfg.Notifier.Port
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("notifier gRPC dial %s: %w", target, err)
	}
	return notifierpb.NewNotifierServiceClient(conn), conn.Close, nil
}

// ResolveInvoiceNotifierTarget picks provider and recipient for invoice delivery.
func ResolveInvoiceNotifierTarget(cfg *config.Config) (notifierpb.Provider, string) {
	if cfg == nil {
		return notifierpb.Provider_PROVIDER_UNSPECIFIED, ""
	}
	if cfg.Notifier.InvoiceRecipient != "" {
		return mapInvoiceProvider(cfg.Notifier.InvoiceProvider), cfg.Notifier.InvoiceRecipient
	}
	if cfg.Notifier.TelegramChatID != "" {
		return notifierpb.Provider_PROVIDER_TELEGRAM, cfg.Notifier.TelegramChatID
	}
	if cfg.Notifier.SlackWebhookURL != "" {
		return notifierpb.Provider_PROVIDER_SLACK, string(cfg.Notifier.SlackWebhookURL)
	}
	if cfg.Notifier.SMTPSender != "" {
		return notifierpb.Provider_PROVIDER_SMTP, cfg.Notifier.SMTPSender
	}
	return notifierpb.Provider_PROVIDER_UNSPECIFIED, ""
}

func mapInvoiceProvider(raw string) notifierpb.Provider {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "SLACK":
		return notifierpb.Provider_PROVIDER_SLACK
	case "SMTP":
		return notifierpb.Provider_PROVIDER_SMTP
	case "SMS":
		return notifierpb.Provider_PROVIDER_SMS
	default:
		return notifierpb.Provider_PROVIDER_TELEGRAM
	}
}
