package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"espx/pkg/broker/client"

	"github.com/google/uuid"
)

const DefaultCampaignUpdateBrokerTopic = "campaigns:update"

// CampaignUpdateWatcherConfig configures the optional broker fallback for campaigns:update (M14-03).
type CampaignUpdateWatcherConfig struct {
	Registry       *Registry
	BrokerURL      string
	BrokerRedisURL string
	BrokerTopic    string
	BrokerGroup    string
	BrokerTimeout  time.Duration
}

// CampaignUpdateWatcher consumes broker campaigns:update when shard-0 Redis pub/sub is down.
type CampaignUpdateWatcher struct {
	cfg CampaignUpdateWatcherConfig
}

// NewCampaignUpdateWatcher constructs a cold-path campaign-update broker listener.
func NewCampaignUpdateWatcher(cfg CampaignUpdateWatcherConfig) *CampaignUpdateWatcher {
	if cfg.BrokerTopic == "" {
		cfg.BrokerTopic = DefaultCampaignUpdateBrokerTopic
	}
	if cfg.BrokerTimeout <= 0 {
		cfg.BrokerTimeout = 3 * time.Second
	}
	if cfg.BrokerGroup == "" {
		host, _ := os.Hostname()
		cfg.BrokerGroup = "campaign-update-" + host
	}
	return &CampaignUpdateWatcher{cfg: cfg}
}

// Start runs until ctx is cancelled.
func (w *CampaignUpdateWatcher) Start(ctx context.Context) {
	if w.cfg.Registry == nil || w.cfg.BrokerURL == "" {
		return
	}
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.consumeOnce(ctx); err != nil {
			slog.Warn("campaign update broker listener error", "error", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func (w *CampaignUpdateWatcher) consumeOnce(ctx context.Context) error {
	cli := client.NewClient(w.cfg.BrokerURL, w.cfg.BrokerTimeout)
	if w.cfg.BrokerRedisURL != "" {
		cli.SetRedisURL(w.cfg.BrokerRedisURL)
	}
	if err := cli.Connect(); err != nil {
		return err
	}
	defer cli.Close()

	const partition uint16 = 0
	start, err := cli.CommittedOffset(w.cfg.BrokerTopic, partition, w.cfg.BrokerGroup)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		iter, err := cli.Fetch(w.cfg.BrokerTopic, partition, start, 64*1024)
		if err != nil {
			return err
		}

		var nextOffset uint64
		got := false
		for iter.Next() {
			got = true
			payload := iter.Payload
			id := uuid.UUID{}
			if !ParseUUID(payload, &id) {
				slog.Warn("campaign update broker payload invalid", "payload", string(payload))
				nextOffset = iter.Offset + 1
				continue
			}
			if err := w.cfg.Registry.UpdateAndWarmCampaign(ctx, id); err != nil {
				slog.Error("campaign update broker reload failed", "campaign_id", id, "error", err)
			} else {
				w.cfg.Registry.MarkPubSubOK()
				slog.Debug("campaign registry reload from broker", "campaign_id", id)
			}
			nextOffset = iter.Offset + 1
		}

		if got && nextOffset > start {
			stored, commitErr := cli.CommitOffset(w.cfg.BrokerTopic, partition, w.cfg.BrokerGroup, nextOffset)
			if commitErr != nil {
				return commitErr
			}
			start = stored
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// PublishCampaignUpdateBroker emits a campaign ID on the broker topic (management cold path).
func PublishCampaignUpdateBroker(brokerURL, brokerRedisURL, topic string, timeout time.Duration, campaignID string) error {
	if brokerURL == "" || campaignID == "" {
		return nil
	}
	if topic == "" {
		topic = DefaultCampaignUpdateBrokerTopic
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	cli := client.NewClient(brokerURL, timeout)
	if brokerRedisURL != "" {
		cli.SetRedisURL(brokerRedisURL)
	}
	if err := cli.Connect(); err != nil {
		return fmt.Errorf("campaign update broker connect: %w", err)
	}
	defer cli.Close()
	_, err := cli.Produce(topic, 0, []byte(campaignID))
	if err != nil {
		return fmt.Errorf("campaign update broker produce: %w", err)
	}
	return nil
}
