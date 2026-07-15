package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"espx/pkg/broker/client"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SlotMapWatcherConfig configures cold-path reload for Fixed Slot Map (Phase 2.2).
type SlotMapWatcherConfig struct {
	Pool           *pgxpool.Pool
	Sharder        *StaticSlotSharder
	NumShards      int
	PollInterval   time.Duration
	BrokerURL      string
	BrokerRedisURL string
	BrokerTopic    string
	BrokerGroup    string
	BrokerTimeout  time.Duration
}

// SlotMapWatcher reloads the slot map from Postgres on poll and broker signals.
type SlotMapWatcher struct {
	cfg SlotMapWatcherConfig
}

// NewSlotMapWatcher constructs a cold-path slot map watcher.
func NewSlotMapWatcher(cfg SlotMapWatcherConfig) *SlotMapWatcher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 10 * time.Second
	}
	if cfg.BrokerTopic == "" {
		cfg.BrokerTopic = DefaultSlotMapReloadTopic
	}
	if cfg.BrokerTimeout <= 0 {
		cfg.BrokerTimeout = 3 * time.Second
	}
	if cfg.BrokerGroup == "" {
		host, _ := os.Hostname()
		cfg.BrokerGroup = "slotmap-" + host
	}
	return &SlotMapWatcher{cfg: cfg}
}

// Start runs poll and optional broker listener until ctx is cancelled.
func (w *SlotMapWatcher) Start(ctx context.Context) {
	if w.cfg.Sharder == nil || w.cfg.Pool == nil {
		return
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.pollLoop(ctx)
	}()

	if w.cfg.BrokerURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.brokerLoop(ctx)
		}()
	}

	wg.Wait()
}

func (w *SlotMapWatcher) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	w.tryReload(ctx, "startup")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tryReload(ctx, "poll")
		}
	}
}

func (w *SlotMapWatcher) brokerLoop(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.consumeBrokerOnce(ctx); err != nil {
			slog.Warn("slot map broker listener error", "error", err, "backoff", backoff)
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

func (w *SlotMapWatcher) consumeBrokerOnce(ctx context.Context) error {
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
			msg, err := DecodeSlotMapReloadMessage(iter.Payload)
			if err != nil {
				slog.Warn("slot map broker payload invalid", "error", err)
				nextOffset = iter.Offset + 1
				continue
			}
			slog.Info("slot map reload signal from broker", "version", msg.Version, "group", w.cfg.BrokerGroup)
			w.tryReload(ctx, "broker")
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

func (w *SlotMapWatcher) tryReload(ctx context.Context, source string) {
	version, changed, err := ReloadStaticSlotMapIfChanged(ctx, w.cfg.Pool, w.cfg.Sharder, w.cfg.NumShards)
	if err != nil {
		slog.Warn("slot map reload failed", "source", source, "error", err)
		return
	}
	if changed {
		slog.Info("slot map reloaded", "source", source, "version", version)
	}
}

// PublishSlotMapReload emits a broker control message after active_version cutover.
func PublishSlotMapReload(brokerURL, brokerRedisURL, topic string, timeout time.Duration, version int32) error {
	if brokerURL == "" {
		return nil
	}
	if topic == "" {
		topic = DefaultSlotMapReloadTopic
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	payload, err := EncodeSlotMapReloadMessage(version)
	if err != nil {
		return err
	}
	cli := client.NewClient(brokerURL, timeout)
	if brokerRedisURL != "" {
		cli.SetRedisURL(brokerRedisURL)
	}
	if err := cli.Connect(); err != nil {
		return fmt.Errorf("slot map reload publish connect: %w", err)
	}
	defer cli.Close()
	if _, err := cli.Produce(topic, 0, payload); err != nil {
		return fmt.Errorf("slot map reload publish: %w", err)
	}
	slog.Info("published slot map reload", "topic", topic, "version", version)
	return nil
}
