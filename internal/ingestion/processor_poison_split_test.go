package ingestion

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"espx/internal/campaignmodel"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type batchPoisonStore struct {
	poisonID uuid.UUID
	calls    int
}

func (s *batchPoisonStore) StoreBatch(ctx context.Context, events []*campaignmodel.Event) error {
	s.calls++
	if len(events) == 0 {
		return nil
	}
	for _, e := range events {
		if e.CampaignID == s.poisonID {
			if len(events) == 1 {
				return errors.New("single-row poison")
			}
			return errors.New("batch contains poison")
		}
	}
	return nil
}

func (s *batchPoisonStore) Close() error { return nil }

func TestSplitStoreBatch_BinarySplitNotPerRow(t *testing.T) {
	t.Parallel()

	poison := uuid.New()
	batch := make([]*campaignmodel.Event, 8)
	msgIDs := make([]string, 8)
	for i := range batch {
		id := uuid.New()
		if i == 3 {
			id = poison
		}
		batch[i] = &campaignmodel.Event{CampaignID: id, Type: "click"}
		msgIDs[i] = fmt.Sprintf("%d-0", i)
	}

	store := &batchPoisonStore{poisonID: poison}
	consumer := &StreamConsumer{store: store, writeTimeout: 10 * time.Second}

	okIdx, failIdx := consumer.splitStoreBatch(context.Background(), batch, msgIDs, 0)

	assert.Len(t, okIdx, 7)
	require.Equal(t, []int{3}, failIdx)
	assert.Less(t, store.calls, len(batch), "binary split should beat per-row O(n) StoreBatch calls on multi-event batches")
}
