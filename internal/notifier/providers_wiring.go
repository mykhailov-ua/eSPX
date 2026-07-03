package notifier

import (
	"time"

	"espx/internal/config"
	"espx/internal/notifier/pb"
)

// NewProvidersFromConfig wires per-channel providers with isolated circuit breakers from startup config.
func NewProvidersFromConfig(cfg *config.Config) map[pb.Provider]Provider {
	if cfg == nil {
		return nil
	}

	n := cfg.Notifier
	failThreshold := int64(n.BreakerFailThreshold)
	successThreshold := int64(n.BreakerSuccessThreshold)
	openTimeout := time.Duration(n.BreakerOpenTimeoutMs) * time.Millisecond

	telegramBreaker := NewCircuitBreaker(failThreshold, successThreshold, openTimeout)
	slackBreaker := NewCircuitBreaker(failThreshold, successThreshold, openTimeout)
	smtpBreaker := NewCircuitBreaker(failThreshold, successThreshold, openTimeout)
	smsBreaker := NewCircuitBreaker(failThreshold, successThreshold, openTimeout)

	return map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: NewTelegramProvider(
			string(n.TelegramBotToken),
			n.TelegramChatID,
			telegramBreaker,
		),
		pb.Provider_PROVIDER_SLACK: NewSlackProvider(
			string(n.SlackWebhookURL),
			slackBreaker,
		),
		pb.Provider_PROVIDER_SMTP: NewSMTPProvider(
			n.SMTPHost,
			n.SMTPPort,
			n.SMTPUsername,
			string(n.SMTPPassword),
			n.SMTPSender,
			smtpBreaker,
		),
		pb.Provider_PROVIDER_SMS: NewSMSProvider(
			n.SMSProviderURL,
			string(n.SMSAPIToken),
			n.SMSDefaultRecipient,
			smsBreaker,
		),
	}
}
