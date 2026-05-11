package ads

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

type StreamProducer struct {
	rdb          redis.UniversalClient
	streamName   string
	maxStreamLen int64
	writeTimeout time.Duration
}

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

	_, err := p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		MaxLen: p.maxStreamLen,
		Approx: true,
		Values: map[string]interface{}{
			"click_id":    evt.ClickID,
			"campaign_id": evt.CampaignID.String(),
			"type":        evt.Type,
			"payload":     evt.Payload,
			"ip":          evt.IP,
			"ua":          evt.UA,
		},
	}).Result()

	if err != nil {
		EventsDropped.Inc()
		return err
	}

	EventsProcessed.Inc()
	return nil
}
