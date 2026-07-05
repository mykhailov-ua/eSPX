package notifier

import (
	"context"
	"time"

	"espx/internal/config"
	"espx/internal/notifier/pb"
)

// ProviderBundle wires delivery providers and their circuit breakers for runtime metrics.
type ProviderBundle struct {
	Providers map[pb.Provider]Provider
	Breakers  map[pb.Provider]*CircuitBreaker
}

func isProdEnv(env string) bool {
	return env == "production" || env == "prod"
}

// NewProvidersFromConfig wires per-channel providers with isolated circuit breakers from startup config.
func NewProvidersFromConfig(cfg *config.Config) map[pb.Provider]Provider {
	return NewProviderBundleFromConfig(cfg).Providers
}

// NewProviderBundleFromConfig wires providers and breakers for delivery and observability.
func NewProviderBundleFromConfig(cfg *config.Config) ProviderBundle {
	if cfg == nil {
		return ProviderBundle{}
	}

	n := cfg.Notifier
	failThreshold := int64(n.BreakerFailThreshold)
	successThreshold := int64(n.BreakerSuccessThreshold)
	openTimeout := time.Duration(n.BreakerOpenTimeoutMs) * time.Millisecond
	requireCredentials := isProdEnv(cfg.Env)

	telegramBreaker := NewCircuitBreaker(failThreshold, successThreshold, openTimeout)
	slackBreaker := NewCircuitBreaker(failThreshold, successThreshold, openTimeout)
	smtpBreaker := NewCircuitBreaker(failThreshold, successThreshold, openTimeout)
	smsBreaker := NewCircuitBreaker(failThreshold, successThreshold, openTimeout)

	return ProviderBundle{
		Providers: map[pb.Provider]Provider{
			pb.Provider_PROVIDER_TELEGRAM: NewTelegramProvider(
				string(n.TelegramBotToken),
				n.TelegramChatID,
				telegramBreaker,
				requireCredentials,
			),
			pb.Provider_PROVIDER_SLACK: NewSlackProvider(
				string(n.SlackWebhookURL),
				slackBreaker,
				requireCredentials,
			),
			pb.Provider_PROVIDER_SMTP: NewSMTPProvider(
				n.SMTPHost,
				n.SMTPPort,
				n.SMTPUsername,
				string(n.SMTPPassword),
				n.SMTPSender,
				smtpBreaker,
				requireCredentials,
			),
			pb.Provider_PROVIDER_SMS: NewSMSProvider(
				n.SMSProviderURL,
				string(n.SMSAPIToken),
				n.SMSDefaultRecipient,
				smsBreaker,
				requireCredentials,
			),
		},
		Breakers: map[pb.Provider]*CircuitBreaker{
			pb.Provider_PROVIDER_TELEGRAM: telegramBreaker,
			pb.Provider_PROVIDER_SLACK:    slackBreaker,
			pb.Provider_PROVIDER_SMTP:     smtpBreaker,
			pb.Provider_PROVIDER_SMS:      smsBreaker,
		},
	}
}

// StartCircuitBreakerMetricsScraper publishes breaker state gauges until ctx is cancelled.
func StartCircuitBreakerMetricsScraper(ctx context.Context, breakers map[pb.Provider]*CircuitBreaker, interval time.Duration) {
	if len(breakers) == 0 {
		return
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}

	scrape := func() {
		for provider, breaker := range breakers {
			if breaker == nil {
				continue
			}
			recordCircuitBreakerState(providerName(provider), breaker.State())
		}
	}

	scrape()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scrape()
		}
	}
}

func providerName(provider pb.Provider) string {
	switch provider {
	case pb.Provider_PROVIDER_TELEGRAM:
		return "TELEGRAM"
	case pb.Provider_PROVIDER_SLACK:
		return "SLACK"
	case pb.Provider_PROVIDER_SMTP:
		return "SMTP"
	case pb.Provider_PROVIDER_SMS:
		return "SMS"
	default:
		return "UNSPECIFIED"
	}
}
