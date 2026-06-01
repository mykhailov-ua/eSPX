package logger

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	LogShardsSaturationRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "log_shards_saturation_ratio",
			Help: "Saturation level of the lock-free ring buffer shards.",
		},
		[]string{"shard_id"},
	)

	LogNVMEWriteDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "log_nvme_write_duration_seconds",
			Help:    "Latency of NVMe physical write operations.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		},
	)

	LogLoadSheddingEventsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_load_shedding_events_total",
			Help: "Total count of dropped logs.",
		},
	)

	LogDiskDegraded = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_disk_degraded",
			Help: "Disk degradation status flag.",
		},
	)

	LogRotationTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_rotation_total",
			Help: "Total number of log rotations.",
		},
	)

	LogQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_queue_depth",
			Help: "Logger internal flusher queue depth.",
		},
	)
)

func RegisterMetrics() {
	_ = prometheus.Register(LogShardsSaturationRatio)
	_ = prometheus.Register(LogNVMEWriteDurationSeconds)
	_ = prometheus.Register(LogLoadSheddingEventsTotal)
	_ = prometheus.Register(LogDiskDegraded)
	_ = prometheus.Register(LogRotationTotal)
	_ = prometheus.Register(LogQueueDepth)
}

func (l *Logger) StartMetricsReporter(interval time.Duration) {
	defer l.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.closeChan:
			return
		case <-ticker.C:
			for i, shard := range l.shards {
				wc := atomic.LoadUint64(&shard.writeCursor)
				rc := atomic.LoadUint64(&shard.readCursor)
				var saturation float64
				if wc > rc {
					saturation = float64(wc-rc) / float64(RingCapacity)
				}
				LogShardsSaturationRatio.WithLabelValues(fmt.Sprintf("%d", i)).Set(saturation)
			}
			shedEvents := l.loadSheddingEvents.Swap(0)
			if shedEvents > 0 {
				LogLoadSheddingEventsTotal.Add(float64(shedEvents))
			}
			LogQueueDepth.Set(float64(len(l.persistCh)))
			LogDiskDegraded.Set(float64(l.diskDegraded.Load()))
		}
	}
}
