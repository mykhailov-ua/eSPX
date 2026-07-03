package broker

import (
	"context"
	"sync"
	"time"

	"espx/internal/ads/pb"
	"espx/internal/ads/repo"
	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// producerValuesPool recycles Redis XADD value slices for the stream producer.
var producerValuesPool = sync.Pool{
	New: func() any {
		slice := make([]any, 2)
		slice[0] = "d"
		return &slice
	},
}

// StreamProducer enqueues accepted events onto a Redis stream for async processing.
type StreamProducer struct {
	rdb          redis.UniversalClient
	streamName   string
	maxStreamLen int64
	writeTimeout time.Duration
}

// NewStreamProducer creates a producer with stream trimming sized for consumer lag.
func NewStreamProducer(
	rdb redis.UniversalClient,
	streamName string,
	maxStreamLen int,
	writeTimeout time.Duration,
) *StreamProducer {
	return &StreamProducer{
		rdb:          rdb,
		streamName:   streamName,
		maxStreamLen: int64(maxStreamLen),
		writeTimeout: writeTimeout,
	}
}

// Process marshals and XADDs one event after the track handler accepts it.
func (p *StreamProducer) Process(evt *domain.Event) error {
	if evt.ClickID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		evt.ClickID = id.String()
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer cancel()

	pbEvt := repo.StreamEventPool.Get().(*pb.AdStreamEvent)
	pbEvt.ClickId = repo.UnsafeBytes(evt.ClickID)
	pbEvt.CampaignId = evt.CampaignID[:]
	pbEvt.EventType = repo.UnsafeBytes(evt.Type)
	pbEvt.Payload = evt.Payload
	pbEvt.Ip = repo.UnsafeBytes(evt.IP)
	pbEvt.Ua = repo.UnsafeBytes(evt.UA)
	pbEvt.UserId = repo.UnsafeBytes(evt.UserID)
	pbEvt.CreatedAtUnix = evt.CreatedAt.Unix()
	pbEvt.FraudScore = evt.FraudScore
	pbEvt.FraudReason = repo.UnsafeBytes(evt.FraudReason)
	pbEvt.GhostEvent = evt.GhostEvent

	size := pbEvt.SizeVT()
	bufPtr := repo.ByteBufPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	n, err := pbEvt.MarshalToSizedBufferVT(buf)
	if err != nil {
		repo.ClearAdStreamEvent(pbEvt)
		repo.StreamEventPool.Put(pbEvt)
		*bufPtr = buf
		repo.ByteBufPool.Put(bufPtr)
		metrics.EventsDropped.Inc()
		return err
	}
	data := buf[:n]

	valuesPtr := producerValuesPool.Get().(*[]any)
	values := *valuesPtr

	wrap := repo.ByteSliceValuePool.Get().(*repo.ByteSliceValue)
	wrap.SetBytes(data)
	values[1] = wrap

	_, err = p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		MaxLen: p.maxStreamLen,
		Approx: true,
		Values: values,
	}).Result()

	repo.ClearAdStreamEvent(pbEvt)
	repo.StreamEventPool.Put(pbEvt)
	*bufPtr = buf
	repo.ByteBufPool.Put(bufPtr)
	repo.ByteSliceValuePool.Put(wrap)
	producerValuesPool.Put(valuesPtr)

	if err != nil {
		metrics.EventsDropped.Inc()
		return err
	}

	metrics.EventsProcessed.Inc()
	return nil
}
