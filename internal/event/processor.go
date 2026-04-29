package event

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
)

const (
	maxRetries  = 3
	initialWait = 100 * time.Millisecond
	maxWait     = 2 * time.Second
)

type Event struct {
	ID         uuid.UUID
	CampaignID uuid.UUID
	Type       string
	Payload    []byte
	IP         string
	UA         string
}

type BatchWriter interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

type Processor struct {
	writer       BatchWriter
	ch           chan Event
	batchSize    int
	flushInt     time.Duration
	writeTimeout time.Duration
	maxWorkers   int
	wg           sync.WaitGroup
}

func NewProcessor(writer BatchWriter, batchSize int, maxWorkers int, flushInt, writeTimeout time.Duration) *Processor {
	return &Processor{
		writer:       writer,
		ch:           make(chan Event, batchSize*maxWorkers),
		batchSize:    batchSize,
		flushInt:     flushInt,
		writeTimeout: writeTimeout,
		maxWorkers:   maxWorkers,
	}
}

var ErrBufferFull = errors.New("event buffer is full")

func (p *Processor) Process(evt Event) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	evt.ID = id

	select {
	case p.ch <- evt:
		metrics.EventsProcessed.Inc()
		metrics.ProcessorBufferUsage.Set(float64(len(p.ch)))
		return nil
	default:
		metrics.EventsDropped.Inc()
		return ErrBufferFull
	}
}

func (p *Processor) Start(ctx context.Context) {
	for i := 0; i < p.maxWorkers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

func (p *Processor) worker(ctx context.Context) {
	defer p.wg.Done()
	batch := make([]Event, 0, p.batchSize)
	ticker := time.NewTicker(p.flushInt)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
		drainLoop:
			for {
				select {
				case evt := <-p.ch:
					batch = append(batch, evt)
					if len(batch) >= p.batchSize {
						p.flush(batch)
						p.ClearBatch(&batch)
					}
				default:
					break drainLoop
				}
			}
			if len(batch) > 0 {
				p.flush(batch)
			}
			return
		case evt := <-p.ch:
			batch = append(batch, evt)
			if len(batch) >= p.batchSize {
				p.flush(batch)
				p.ClearBatch(&batch)
				ticker.Reset(p.flushInt)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.flush(batch)
				p.ClearBatch(&batch)
			}
		}
	}
}

// Close signals workers to drain and stop
func (p *Processor) Close() {
	close(p.ch)
}

func (p *Processor) Wait() {
	p.wg.Wait()
}

func (p *Processor) ClearBatch(batch *[]Event) {
	for i := range *batch {
		(*batch)[i].Payload = nil
		(*batch)[i].IP = ""
		(*batch)[i].UA = ""
	}
	*batch = (*batch)[:0]
}

type eventCopySource struct {
	rows []Event
	idx  int
	now  time.Time
	row  []any
}

func (s *eventCopySource) Next() bool {
	s.idx++
	return s.idx < len(s.rows)
}

func (s *eventCopySource) Values() ([]any, error) {
	evt := &s.rows[s.idx]
	s.row[0] = pgtype.UUID{Bytes: evt.ID, Valid: true}
	s.row[1] = pgtype.UUID{Bytes: evt.CampaignID, Valid: true}
	s.row[2] = evt.Type
	s.row[3] = evt.Payload
	s.row[4] = evt.IP
	s.row[5] = evt.UA
	s.row[6] = s.now
	return s.row, nil
}

func (s *eventCopySource) Err() error {
	return nil
}

func (p *Processor) flush(batch []Event) {
	if len(batch) == 0 {
		return
	}

	var err error
	waitTime := initialWait

	for i := 0; i <= maxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
		source := &eventCopySource{
			rows: batch,
			idx:  -1,
			now:  time.Now(),
			row:  make([]any, 7),
		}

		start := time.Now()
		_, err = p.writer.CopyFrom(
			dbCtx,
			pgx.Identifier{"events"},
			[]string{"id", "campaign_id", "event_type", "payload", "ip_address", "user_agent", "created_at"},
			source,
		)
		duration := time.Since(start).Seconds()
		cancel()

		if err == nil {
			metrics.DbWriteDuration.WithLabelValues("copy_from").Observe(duration)
			if i > 0 {
				slog.Info("successfully flushed event batch after retry", "attempts", i+1, "size", len(batch))
			}
			return
		}

		if i < maxRetries {
			slog.Warn("failed to flush event batch, retrying...",
				"error", err,
				"attempt", i+1,
				"wait", waitTime,
				"size", len(batch),
			)
			time.Sleep(waitTime)
			waitTime *= 2
			if waitTime > maxWait {
				waitTime = maxWait
			}
		}
	}

	metrics.DbWriteErrors.WithLabelValues("copy_from").Inc()
	slog.Error("all retries failed for event batch, data lost", "error", err, "size", len(batch))
}
