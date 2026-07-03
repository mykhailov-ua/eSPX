package broker

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"espx/internal/metrics"
	"espx/pkg/broker/client"
	"espx/pkg/broker/protocol"
	redis "github.com/redis/go-redis/v9"
)

// BrokerReconcileConfig compares Redis stream depth with broker consumer progress.
type BrokerReconcileConfig struct {
	BrokerAddr          string
	BrokerRedis         string
	Topic               string
	PartitionCount      int
	BrokerGroup         string
	StreamName          string
	Interval            time.Duration
	DivergenceThreshold uint64
}

// BrokerReconcileWorker publishes ingest divergence gauges for shadow validation.
type BrokerReconcileWorker struct {
	cfg    BrokerReconcileConfig
	shards []redis.UniversalClient
	cli    *client.Client
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBrokerReconcileWorker samples broker vs Redis ingest depth periodically.
func NewBrokerReconcileWorker(cfg BrokerReconcileConfig, shards []redis.UniversalClient) *BrokerReconcileWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.DivergenceThreshold == 0 {
		cfg.DivergenceThreshold = 1000
	}
	if cfg.PartitionCount <= 0 {
		cfg.PartitionCount = 1
	}
	return &BrokerReconcileWorker{
		cfg:    cfg,
		shards: shards,
		cli:    client.NewClient(cfg.BrokerAddr, 5*time.Second),
	}
}

// Start launches the reconciliation sampling loop.
func (w *BrokerReconcileWorker) Start(ctx context.Context) {
	if w.cfg.BrokerAddr == "" || len(w.shards) == 0 {
		return
	}
	if w.cfg.BrokerRedis != "" {
		w.cli.SetRedisURL(w.cfg.BrokerRedis)
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run(runCtx)
	}()
}

// Close stops the reconciliation worker.
func (w *BrokerReconcileWorker) Close() {
	if w.cancel != nil {
		w.cancel()
	}
}

// Wait blocks until the worker exits.
func (w *BrokerReconcileWorker) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *BrokerReconcileWorker) run(ctx context.Context) {
	if err := w.cli.Connect(); err != nil {
		slog.Error("broker reconcile connect failed", "error", err)
		return
	}
	defer func() { _ = w.cli.Close() }()

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sample(ctx)
		}
	}
}

func (w *BrokerReconcileWorker) sample(ctx context.Context) {
	sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var streamLen int64
	for _, shard := range w.shards {
		n, err := shard.XLen(sampleCtx, w.cfg.StreamName).Result()
		if err != nil {
			slog.Warn("broker reconcile XLEN failed", "stream", w.cfg.StreamName, "error", err)
			return
		}
		streamLen += n
	}

	var committedSum uint64
	for p := 0; p < w.cfg.PartitionCount; p++ {
		part := uint16(p)
		committed, err := w.cli.CommittedOffset(w.cfg.Topic, part, w.cfg.BrokerGroup)
		if err != nil {
			slog.Warn("broker reconcile committed offset failed", "partition", part, "group", w.cfg.BrokerGroup, "error", err)
			return
		}
		committedSum += committed
	}

	var brokerHWMSum uint64
	for p := 0; p < w.cfg.PartitionCount; p++ {
		part := uint16(p)
		tpKey := protocol.TopicPartitionID(w.cfg.Topic, part)
		hwmStr, err := w.shards[0].Get(sampleCtx, "espx:topics:"+tpKey+":log_hwm").Result()
		if err == nil {
			if v, parseErr := strconv.ParseUint(hwmStr, 10, 64); parseErr == nil {
				brokerHWMSum += v
				continue
			}
		}
		committed, _ := w.cli.CommittedOffset(w.cfg.Topic, part, w.cfg.BrokerGroup)
		iter, fetchErr := w.cli.Fetch(w.cfg.Topic, part, committed, 1024)
		if fetchErr == nil {
			brokerHWMSum += iter.HighWatermark
		}
	}

	var brokerLag uint64
	if brokerHWMSum > committedSum {
		brokerLag = brokerHWMSum - committedSum
	}
	metrics.BrokerConsumerLagMessages.WithLabelValues(w.cfg.Topic, w.cfg.BrokerGroup).Set(float64(brokerLag))

	var divergence int64
	if streamLen > int64(committedSum) {
		divergence = streamLen - int64(committedSum)
	}
	metrics.BrokerIngestDivergenceMessages.WithLabelValues(w.cfg.Topic, w.cfg.BrokerGroup).Set(float64(divergence))

	if uint64(divergence) > w.cfg.DivergenceThreshold {
		metrics.BrokerIngestDivergenceHigh.WithLabelValues(w.cfg.Topic, w.cfg.BrokerGroup).Set(1)
	} else {
		metrics.BrokerIngestDivergenceHigh.WithLabelValues(w.cfg.Topic, w.cfg.BrokerGroup).Set(0)
	}
}
