package notifier

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Worker polls the notification queue and delivers pending notifications.
type Worker struct {
	service   *Service
	interval  time.Duration
	batchSize int32
	wg        sync.WaitGroup
}

// NewWorker returns a polling worker; interval and batchSize fall back to package defaults when non-positive.
func NewWorker(service *Service, interval time.Duration, batchSize int32) *Worker {
	if interval <= 0 {
		interval = time.Second
	}
	if batchSize <= 0 {
		batchSize = workerBatchSize
	}
	return &Worker{
		service:   service,
		interval:  interval,
		batchSize: batchSize,
	}
}

// Start runs the polling loop in a background goroutine until ctx is cancelled.
func (worker *Worker) Start(ctx context.Context) {
	worker.wg.Add(1)
	go func() {
		defer worker.wg.Done()

		slog.Info("notification worker starting polling loop", "interval", worker.interval, "batch_size", worker.batchSize)

		timer := time.NewTimer(worker.interval)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				slog.Info("notification worker stopping polling loop")
				return
			case <-timer.C:
				processed, err := worker.service.ProcessPending(ctx, worker.batchSize)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					workerIterationErrorsTotal.Inc()
					slog.Error("notification worker processing iteration failed", "error", err, "retry_in", workerErrorBackoff)
					timer.Reset(workerErrorBackoff)
					continue
				}

				if processed > 0 {
					workerBatchProcessed.Observe(float64(processed))
					timer.Reset(0)
				} else {
					timer.Reset(worker.interval)
				}
			}
		}
	}()
}

func (worker *Worker) Wait() {
	worker.wg.Wait()
}

// StartPool runs N independent polling loops for higher throughput.
func (worker *Worker) StartPool(ctx context.Context, concurrency int) {
	if concurrency <= 1 {
		worker.Start(ctx)
		return
	}
	for range concurrency {
		worker.Start(ctx)
	}
}
