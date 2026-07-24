package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"espx/internal/campaignmodel"

	redis "github.com/redis/go-redis/v9"
)

// filterEvalPinSlot is one sticky eval connection for a pinned worker on one shard.
type filterEvalPinSlot struct {
	client *redis.Client
	conn   *redis.Conn
}

// filterEvalPin holds sticky redis.Conn values per pinned worker and shard.
// Each conn is not thread-safe; only the owning worker goroutine may use its row.
type filterEvalPin struct {
	shards  int
	workers int
	slots   []filterEvalPinSlot
}

func (p *filterEvalPin) slot(worker, shard int) *filterEvalPinSlot {
	if p == nil || worker < 0 || worker >= p.workers || shard < 0 || shard >= p.shards {
		return nil
	}
	return &p.slots[worker*p.shards+shard]
}

func (p *filterEvalPin) conn(worker, shard int) *redis.Conn {
	s := p.slot(worker, shard)
	if s == nil {
		return nil
	}
	return s.conn
}

// SetFilterEvalPinWorkers configures sticky eval connections opened by PreloadScripts.
// workers must match PinnedWorkerPool size when offload is enabled.
func (f *UnifiedFilter) SetFilterEvalPinWorkers(workers int) {
	if f == nil {
		return
	}
	if workers < 0 {
		workers = 0
	}
	f.evalPinWorkers = workers
}

// FilterEvalPinWorkers returns the configured sticky eval worker row count.
func (f *UnifiedFilter) FilterEvalPinWorkers() int {
	if f == nil {
		return 0
	}
	return f.evalPinWorkers
}

func (f *UnifiedFilter) evalPinConn(evt *campaignmodel.Event, shard int) *redis.Conn {
	if f == nil || f.evalPins == nil || evt == nil || evt.FilterWorkerIdx < 0 {
		return nil
	}
	worker := int(evt.FilterWorkerIdx)
	if worker >= f.evalPinWorkers {
		return nil
	}
	return f.evalPins.conn(worker, shard)
}

func isStickyConnRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, redis.ErrClosed) || errors.Is(err, io.EOF) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "closed") ||
		strings.Contains(s, "EOF") ||
		strings.Contains(s, "bad state") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "reset by peer")
}

// processFilterEval runs one Redis command on a sticky pin when assigned, with one reopen retry.
func (f *UnifiedFilter) processFilterEval(ctx context.Context, c redis.UniversalClient, shard int, evt *campaignmodel.Event, cmd redis.Cmder) error {
	pin := f.evalPinConn(evt, shard)
	err := processRedisCmd(ctx, c, pin, cmd)
	if err == nil || pin == nil || evt == nil || evt.FilterWorkerIdx < 0 {
		return err
	}
	if !isStickyConnRetryable(err) {
		return err
	}
	worker := int(evt.FilterWorkerIdx)
	if reopenErr := f.reopenEvalPin(ctx, worker, shard); reopenErr != nil {
		return err
	}
	pin = f.evalPinConn(evt, shard)
	return processRedisCmd(ctx, c, pin, cmd)
}

func (f *UnifiedFilter) openFilterEvalPins(ctx context.Context) error {
	if f == nil || f.evalPinWorkers <= 0 || len(f.rdbs) == 0 {
		return nil
	}
	workers := f.evalPinWorkers
	shards := len(f.rdbs)
	pin := &filterEvalPin{
		workers: workers,
		shards:  shards,
		slots:   make([]filterEvalPinSlot, workers*shards),
	}
	for w := 0; w < workers; w++ {
		for i, rdb := range f.rdbs {
			client, ok := rdb.(*redis.Client)
			if !ok {
				continue
			}
			slot := pin.slot(w, i)
			slot.client = client
			slot.conn = client.Conn()
			if err := slot.conn.Ping(ctx).Err(); err != nil {
				f.closeFilterEvalPins()
				return fmt.Errorf("ping filter eval pin worker=%d shard=%d: %w", w, i, err)
			}
		}
	}
	f.evalPins = pin
	return nil
}

func (f *UnifiedFilter) reopenEvalPin(ctx context.Context, worker, shard int) error {
	if f == nil || f.evalPins == nil {
		return fmt.Errorf("filter eval pins not open")
	}
	slot := f.evalPins.slot(worker, shard)
	if slot == nil || slot.client == nil {
		return fmt.Errorf("filter eval pin worker=%d shard=%d unavailable", worker, shard)
	}
	if slot.conn != nil {
		_ = slot.conn.Close()
		slot.conn = nil
	}
	slot.conn = slot.client.Conn()
	if err := slot.conn.Ping(ctx).Err(); err != nil {
		return err
	}
	return nil
}

// CloseFilterEvalPins closes sticky eval connections opened by PreloadScripts.
func (f *UnifiedFilter) CloseFilterEvalPins() {
	if f == nil {
		return
	}
	f.closeFilterEvalPins()
}

func (f *UnifiedFilter) closeFilterEvalPins() {
	if f == nil || f.evalPins == nil {
		return
	}
	for i := range f.evalPins.slots {
		if f.evalPins.slots[i].conn != nil {
			_ = f.evalPins.slots[i].conn.Close()
			f.evalPins.slots[i].conn = nil
		}
	}
	f.evalPins = nil
}

// processRedisCmd runs one pooled command on a sticky conn when pinned, else the shard client.
func processRedisCmd(ctx context.Context, c redis.UniversalClient, pin *redis.Conn, cmd redis.Cmder) error {
	if pin != nil {
		return pin.Process(ctx, cmd)
	}
	return c.Process(ctx, cmd)
}
