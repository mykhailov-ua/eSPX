package ingestion

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/ingestion/pb"
	"espx/pkg/broker/client"

	"github.com/google/uuid"
)

const (
	budgetDeltaRingCapacity = 8192
	budgetDeltaRingMask     = budgetDeltaRingCapacity - 1
	budgetDeltaRingUsable   = budgetDeltaRingCapacity - 1
	budgetDeltaFlushBatch   = 256
	budgetDeltaFlushEvery   = 5 * time.Millisecond
)

type budgetDeltaSlot struct {
	ready       atomic.Uint32
	amountMicro int64
	campaignID  uuid.UUID
}

// BudgetDeltaPublisher enqueues local quanta spends for async broker produce (M8-04).
type BudgetDeltaPublisher struct {
	_           [64]byte
	writeCursor uint64
	_           [64]byte
	allocCursor uint64
	_           [64]byte
	readCursor  uint64
	_           [64]byte
	slots       [budgetDeltaRingCapacity]budgetDeltaSlot

	topic     string
	trackerID []byte
	cli       *client.Client
	timeout   time.Duration

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// BudgetDeltaPublisherConfig wires broker produce for budget-deltas topic.
type BudgetDeltaPublisherConfig struct {
	BrokerAddr string
	RedisURL   string
	Topic      string
	TrackerID  string
	Timeout    time.Duration
}

// NewBudgetDeltaPublisher starts the async broker drainer.
func NewBudgetDeltaPublisher(cfg BudgetDeltaPublisherConfig) *BudgetDeltaPublisher {
	if cfg.BrokerAddr == "" || cfg.Topic == "" {
		return nil
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	p := &BudgetDeltaPublisher{
		topic:     cfg.Topic,
		trackerID: []byte(cfg.TrackerID),
		cli:       client.NewClient(cfg.BrokerAddr, cfg.Timeout),
		timeout:   cfg.Timeout,
		stopCh:    make(chan struct{}),
	}
	if cfg.RedisURL != "" {
		p.cli.SetRedisURL(cfg.RedisURL)
	}
	p.wg.Add(1)
	go p.worker()
	return p
}

// Publish enqueues one local spend delta (0 allocs when ring has capacity).
func (p *BudgetDeltaPublisher) Publish(campaignID uuid.UUID, amountMicro int64) {
	if p == nil || amountMicro <= 0 {
		return
	}
	p.enqueue(campaignID, amountMicro)
}

// PublishReturn enqueues a negative delta when unused local quanta are returned (M14-13).
func (p *BudgetDeltaPublisher) PublishReturn(campaignID uuid.UUID, amountMicro int64) {
	if p == nil || amountMicro <= 0 {
		return
	}
	p.enqueue(campaignID, -amountMicro)
}

func (p *BudgetDeltaPublisher) enqueue(campaignID uuid.UUID, amountMicro int64) {
	for {
		alloc := atomic.LoadUint64(&p.allocCursor)
		read := atomic.LoadUint64(&p.readCursor)
		if alloc-read >= budgetDeltaRingUsable {
			return
		}
		if !atomic.CompareAndSwapUint64(&p.allocCursor, alloc, alloc+1) {
			continue
		}
		idx := alloc & budgetDeltaRingMask
		slot := &p.slots[idx]
		if slot.ready.Load() != 0 {
			return
		}
		slot.campaignID = campaignID
		slot.amountMicro = amountMicro
		slot.ready.Store(1)
		atomic.StoreUint64(&p.writeCursor, alloc+1)
		return
	}
}

// Close stops the publisher and drains pending deltas.
func (p *BudgetDeltaPublisher) Close() {
	if p == nil {
		return
	}
	close(p.stopCh)
	p.wg.Wait()
	_ = p.cli.Close()
}

func (p *BudgetDeltaPublisher) worker() {
	defer p.wg.Done()
	if err := p.cli.Connect(); err != nil {
		return
	}
	ticker := time.NewTicker(budgetDeltaFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			p.flush()
			return
		case <-ticker.C:
			p.flush()
		}
	}
}

func (p *BudgetDeltaPublisher) flush() {
	batch := 0
	for batch < budgetDeltaFlushBatch {
		read := atomic.LoadUint64(&p.readCursor)
		write := atomic.LoadUint64(&p.writeCursor)
		if read >= write {
			return
		}
		idx := read & budgetDeltaRingMask
		slot := &p.slots[idx]
		if slot.ready.Load() != 1 {
			return
		}
		p.produce(slot.campaignID, slot.amountMicro)
		slot.ready.Store(0)
		atomic.StoreUint64(&p.readCursor, read+1)
		batch++
	}
}

func (p *BudgetDeltaPublisher) produce(campaignID uuid.UUID, amountMicro int64) {
	msg := budgetDeltaPool.Get().(*pb.BudgetDelta)
	msg.Reset()
	msg.CampaignId = append(msg.CampaignId[:0], campaignID[:]...)
	msg.AmountMicro = amountMicro
	msg.CreatedAtUnixNano = monotonicNano()
	msg.TrackerId = append(msg.TrackerId[:0], p.trackerID...)
	data, err := msg.MarshalVT()
	budgetDeltaPool.Put(msg)
	if err != nil || len(data) == 0 {
		return
	}
	_, _ = p.cli.Produce(p.topic, 0, data)
}

var budgetDeltaPool = sync.Pool{
	New: func() any { return &pb.BudgetDelta{} },
}

// BudgetDeltaAggregator tracks unflushed broker deltas for reconciliation (M8-04).
type BudgetDeltaAggregator struct {
	mu      sync.Mutex
	pending map[uuid.UUID]int64
	flushed map[uuid.UUID]int64
}

// NewBudgetDeltaAggregator returns an empty pending-delta accumulator.
func NewBudgetDeltaAggregator() *BudgetDeltaAggregator {
	return &BudgetDeltaAggregator{
		pending: make(map[uuid.UUID]int64, 256),
		flushed: make(map[uuid.UUID]int64, 256),
	}
}

// Record adds a consumed delta to the pending ledger (negative amounts reduce pending on return).
func (a *BudgetDeltaAggregator) Record(campaignID uuid.UUID, amountMicro int64) {
	if a == nil || amountMicro == 0 {
		return
	}
	a.mu.Lock()
	a.pending[campaignID] += amountMicro
	if a.pending[campaignID] == 0 {
		delete(a.pending, campaignID)
	}
	a.mu.Unlock()
}

// PendingDeltaMicro implements management.BrokerPendingDeltaReader.
func (a *BudgetDeltaAggregator) PendingDeltaMicro(_ context.Context, campaignID uuid.UUID) (int64, error) {
	if a == nil {
		return 0, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pending[campaignID], nil
}

// MarkFlushed moves pending deltas into the flushed tally after Redis/PG sync.
func (a *BudgetDeltaAggregator) MarkFlushed(campaignID uuid.UUID, amountMicro int64) {
	if a == nil || amountMicro <= 0 {
		return
	}
	a.mu.Lock()
	a.pending[campaignID] -= amountMicro
	if a.pending[campaignID] <= 0 {
		delete(a.pending, campaignID)
	}
	a.flushed[campaignID] += amountMicro
	a.mu.Unlock()
}

// BudgetDeltaConsumer reads budget-deltas from broker and feeds the aggregator.
type BudgetDeltaConsumer struct {
	aggregator *BudgetDeltaAggregator
	cfg        BrokerConsumerConfig
	cli        *client.Client
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewBudgetDeltaConsumer starts a cold-path broker reader for budget deltas.
func NewBudgetDeltaConsumer(agg *BudgetDeltaAggregator, cfg BrokerConsumerConfig) *BudgetDeltaConsumer {
	if agg == nil || cfg.BrokerAddr == "" || cfg.Topic == "" {
		return nil
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.IdleWait <= 0 {
		cfg.IdleWait = 250 * time.Millisecond
	}
	c := &BudgetDeltaConsumer{
		aggregator: agg,
		cfg:        cfg,
		cli:        client.NewClient(cfg.BrokerAddr, cfg.Timeout),
	}
	return c
}

// Start launches the fetch loop until Close.
func (c *BudgetDeltaConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}
	if c.cfg.RedisURL != "" {
		c.cli.SetRedisURL(c.cfg.RedisURL)
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run(runCtx)
	}()
}

// Close stops the consumer.
func (c *BudgetDeltaConsumer) Close() {
	if c == nil {
		return
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	_ = c.cli.Close()
}

func (c *BudgetDeltaConsumer) run(ctx context.Context) {
	if err := c.cli.Connect(); err != nil {
		return
	}
	offset, _ := c.cli.CommittedOffset(c.cfg.Topic, c.cfg.Partition, c.cfg.Group)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		iter, err := c.cli.Fetch(c.cfg.Topic, c.cfg.Partition, offset, c.cfg.MaxBytes)
		if err != nil {
			time.Sleep(c.cfg.IdleWait)
			continue
		}
		processed := 0
		for iter.Next() {
			c.ingest(iter.Payload)
			offset = iter.Offset + 1
			processed++
		}
		if processed == 0 {
			time.Sleep(c.cfg.IdleWait)
			continue
		}
		_, _ = c.cli.CommitOffset(c.cfg.Topic, c.cfg.Partition, c.cfg.Group, offset)
	}
}

func (c *BudgetDeltaConsumer) ingest(payload []byte) {
	msg := budgetDeltaPool.Get().(*pb.BudgetDelta)
	msg.Reset()
	if err := msg.UnmarshalVT(payload); err != nil {
		budgetDeltaPool.Put(msg)
		return
	}
	if len(msg.CampaignId) >= 16 {
		var id uuid.UUID
		_ = ParseUUID(msg.CampaignId[:16], &id)
		c.aggregator.Record(id, msg.AmountMicro)
	}
	budgetDeltaPool.Put(msg)
}

// FetchRecoveryDeltas replays broker topic from offset for tracker restart (M8-09).
func FetchRecoveryDeltas(ctx context.Context, cfg BrokerConsumerConfig, startOffset uint64) (map[uuid.UUID]int64, error) {
	out := make(map[uuid.UUID]int64)
	if cfg.BrokerAddr == "" || cfg.Topic == "" {
		return out, nil
	}
	cli := client.NewClient(cfg.BrokerAddr, cfg.Timeout)
	if cfg.RedisURL != "" {
		cli.SetRedisURL(cfg.RedisURL)
	}
	if err := cli.Connect(); err != nil {
		return out, err
	}
	defer func() { _ = cli.Close() }()

	offset := startOffset
	for {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		iter, err := cli.Fetch(cfg.Topic, cfg.Partition, offset, cfg.MaxBytes)
		if err != nil {
			return out, err
		}
		n := 0
		for iter.Next() {
			n++
			delta := budgetDeltaPool.Get().(*pb.BudgetDelta)
			delta.Reset()
			if err := delta.UnmarshalVT(iter.Payload); err == nil && len(delta.CampaignId) >= 16 {
				var id uuid.UUID
				_ = ParseUUID(delta.CampaignId[:16], &id)
				out[id] += delta.AmountMicro
			}
			budgetDeltaPool.Put(delta)
			offset = iter.Offset + 1
		}
		if n == 0 {
			return out, nil
		}
	}
}
