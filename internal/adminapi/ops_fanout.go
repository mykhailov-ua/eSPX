package adminapi

import (
	"context"
	"sync"
	"time"

	"espx/internal/config"

	"espx/internal/metrics"
)

const (
	defaultFanOutMaxConcurrency = 8
	defaultFanOutPerSourceTO    = 2 * time.Second
)

// FanOutSourceError records a single source failure during a parallel admin read.
type FanOutSourceError struct {
	Source string `json:"source"`
	Code   string `json:"code"`
}

// FanOutResult is the generic merge wrapper for multi-source admin reads.
type FanOutResult[T any] struct {
	Items      []T                 `json:"items"`
	Partial    bool                `json:"partial"`
	Errors     []FanOutSourceError `json:"errors,omitempty"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

// FanOutSource describes one parallel poll target for FanOutCollector.
type FanOutSource[T any] struct {
	ID   string
	Poll func(ctx context.Context) ([]T, error)
}

// FanOutCollector runs parallel source polls with per-source timeouts and a concurrency cap.
type FanOutCollector struct {
	maxConcurrency int
	perSourceTO    time.Duration
	route          string
}

// NewFanOutCollector builds a collector from config and a route label for metrics.
func NewFanOutCollector(cfg *config.Config, route string) *FanOutCollector {
	max := defaultFanOutMaxConcurrency
	if cfg != nil && cfg.Management.AdminFanoutMaxConcurrency > 0 {
		max = cfg.Management.AdminFanoutMaxConcurrency
	}
	return &FanOutCollector{
		maxConcurrency: max,
		perSourceTO:    defaultFanOutPerSourceTO,
		route:          route,
	}
}

type fanOutResultSlot[T any] struct {
	sourceID string
	items    []T
	err      error
}

// CollectFanOut polls all sources in parallel and merges successful slices.
func CollectFanOut[T any](ctx context.Context, c *FanOutCollector, sources []FanOutSource[T]) FanOutResult[T] {
	start := time.Now()
	defer func() {
		if c != nil && c.route != "" {
			metrics.AdminFanoutLatencySeconds.WithLabelValues(c.route).Observe(time.Since(start).Seconds())
		}
	}()

	if len(sources) == 0 {
		return FanOutResult[T]{Items: []T{}}
	}
	if c == nil {
		c = NewFanOutCollector(nil, "")
	}

	sem := make(chan struct{}, c.maxConcurrency)
	slots := make([]fanOutResultSlot[T], len(sources))
	var wg sync.WaitGroup

	for i, src := range sources {
		wg.Add(1)
		go func(idx int, source FanOutSource[T]) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			srcCtx, cancel := context.WithTimeout(ctx, c.perSourceTO)
			defer cancel()

			items, err := source.Poll(srcCtx)
			slots[idx] = fanOutResultSlot[T]{sourceID: source.ID, items: items, err: err}
		}(i, src)
	}
	wg.Wait()

	var (
		out    FanOutResult[T]
		ok     int
		failed int
	)
	for _, slot := range slots {
		if slot.err != nil {
			failed++
			code := "SOURCE_UNAVAILABLE"
			if slot.err == context.DeadlineExceeded || slot.err == context.Canceled {
				code = "TIMEOUT"
			}
			out.Errors = append(out.Errors, FanOutSourceError{Source: slot.sourceID, Code: code})
			continue
		}
		ok++
		if len(slot.items) > 0 {
			out.Items = append(out.Items, slot.items...)
		}
	}

	if failed > 0 && ok > 0 {
		out.Partial = true
	}
	if c.route != "" {
		metrics.AdminFanoutSourcesTotal.WithLabelValues(c.route).Add(float64(len(sources)))
		if out.Partial {
			metrics.AdminFanoutPartialTotal.WithLabelValues(c.route).Inc()
		}
	}
	return out
}
