package notifier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"espx/internal/notifier/db"
	"espx/internal/notifier/pb"
)

type broadcastResult struct {
	sentProvider db.NotifierProvider
	partialNote  string
	err          error
}

// defaultBroadcastOrder is the fan-out priority when broadcast_providers is empty.
var defaultBroadcastOrder = []db.NotifierProvider{
	db.NotifierProviderSLACK,
	db.NotifierProviderTELEGRAM,
	db.NotifierProviderSMS,
	db.NotifierProviderSMTP,
}

func (service *Service) resolveBroadcastTargets(stored []db.NotifierProvider) []db.NotifierProvider {
	if len(stored) > 0 {
		return stored
	}

	targets := make([]db.NotifierProvider, 0, len(defaultBroadcastOrder))
	for _, provider := range defaultBroadcastOrder {
		if _, configured := service.providers[MapDBProviderToPB(provider)]; configured {
			targets = append(targets, provider)
		}
	}
	return targets
}

func (service *Service) deliverBroadcast(
	ctx context.Context,
	leadID string,
	primary db.NotifierProvider,
	primaryRecipient string,
	targets []db.NotifierProvider,
	title, body string,
) broadcastResult {
	if len(targets) == 0 {
		return broadcastResult{err: fmt.Errorf("broadcast: no configured providers")}
	}

	type channelResult struct {
		provider db.NotifierProvider
		err      error
	}

	results := make([]channelResult, len(targets))
	sendCtx := context.WithValue(ctx, NotificationIDContextKey, leadID)

	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Add(1)
		go func(idx int, prov db.NotifierProvider) {
			defer wg.Done()
			results[idx] = channelResult{
				provider: prov,
				err:      service.sendViaProvider(sendCtx, prov, primary, primaryRecipient, title, body),
			}
		}(i, target)
	}
	wg.Wait()

	successes := make([]db.NotifierProvider, 0, len(results))
	failures := make([]string, 0, len(results))
	for _, result := range results {
		if result.err == nil {
			successes = append(successes, result.provider)
			continue
		}
		failures = append(failures, fmt.Sprintf("%s: %v", result.provider, result.err))
	}

	if len(successes) == 0 {
		return broadcastResult{err: fmt.Errorf("broadcast failed: %s", strings.Join(failures, "; "))}
	}

	sentProvider := successes[0]
	for _, provider := range successes {
		if provider == primary {
			sentProvider = primary
			break
		}
	}

	var partialNote string
	if len(failures) > 0 {
		partialNote = fmt.Sprintf(
			"broadcast partial (%d/%d ok): %s",
			len(successes),
			len(targets),
			strings.Join(failures, "; "),
		)
		broadcastPartialTotal.Inc()
		slog.Warn("notification broadcast partial success",
			"notification_id", leadID,
			"succeeded", successes,
			"failures", failures,
		)
	}

	return broadcastResult{sentProvider: sentProvider, partialNote: partialNote}
}

func (service *Service) sendViaProvider(
	ctx context.Context,
	target, primary db.NotifierProvider,
	primaryRecipient, title, body string,
) error {
	pbProvider := MapDBProviderToPB(target)
	provider, exists := service.providers[pbProvider]
	if !exists {
		return fmt.Errorf("provider %s not configured", target)
	}

	recipient := ""
	if target == primary {
		recipient = primaryRecipient
	}

	providerName := string(target)
	if service.deliveryRateLimiter != nil && !service.deliveryRateLimiter.Allow(providerName, recipient) {
		recordDelivery(providerName, false, 0)
		return ErrRateLimited
	}

	start := time.Now()
	err := provider.Send(ctx, recipient, title, body)
	if err != nil {
		var rateErr *ProviderRateLimitedError
		if errors.As(err, &rateErr) {
			service.deliveryRateLimiter.Backoff(rateErr.Provider, recipient, rateErr.RetryAfter)
		}
	}
	recordDelivery(providerName, err == nil, time.Since(start).Seconds())
	return err
}

func (service *Service) deliverFallback(
	ctx context.Context,
	leadID string,
	startProvider db.NotifierProvider,
	startRecipient, title, body string,
) (db.NotifierProvider, error) {
	currentProvider := startProvider
	currentRecipient := startRecipient

	for {
		pbProvider := MapDBProviderToPB(currentProvider)
		_, exists := service.providers[pbProvider]
		if !exists {
			sendErr := fmt.Errorf("provider %s not configured", currentProvider)
			nextProvider, fallbackFound := nextConfiguredFallback(service.providers, currentProvider)
			if !fallbackFound {
				return currentProvider, sendErr
			}
			slog.Warn("notification delivery failed, attempting fallback",
				"notification_id", leadID,
				"failed_provider", currentProvider,
				"fallback_provider", nextProvider,
				"error", sendErr,
			)
			currentProvider = nextProvider
			currentRecipient = ""
			continue
		}

		pCtx := context.WithValue(ctx, NotificationIDContextKey, leadID)
		sendErr := service.sendViaProvider(pCtx, currentProvider, currentProvider, currentRecipient, title, body)
		if sendErr == nil {
			return currentProvider, nil
		}

		nextProvider, fallbackFound := nextConfiguredFallback(service.providers, currentProvider)
		if !fallbackFound {
			return currentProvider, sendErr
		}

		slog.Warn("notification delivery failed, attempting fallback",
			"notification_id", leadID,
			"failed_provider", currentProvider,
			"fallback_provider", nextProvider,
			"error", sendErr,
		)

		recordFallback(string(currentProvider), string(nextProvider))

		currentProvider = nextProvider
		currentRecipient = ""
	}
}

func nextConfiguredFallback(providers map[pb.Provider]Provider, current db.NotifierProvider) (db.NotifierProvider, bool) {
	fallbackChain := map[db.NotifierProvider]db.NotifierProvider{
		db.NotifierProviderSLACK:    db.NotifierProviderTELEGRAM,
		db.NotifierProviderTELEGRAM: db.NotifierProviderSMS,
		db.NotifierProviderSMS:      db.NotifierProviderSMTP,
	}

	nextProvider := current
	for {
		var ok bool
		nextProvider, ok = fallbackChain[nextProvider]
		if !ok {
			return "", false
		}
		if _, configured := providers[MapDBProviderToPB(nextProvider)]; configured {
			return nextProvider, true
		}
	}
}
