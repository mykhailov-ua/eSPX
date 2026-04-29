package unit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockBatchWriter struct {
	mu      sync.Mutex
	flushes [][]event.Event
	err     error
}

func (m *MockBatchWriter) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return 0, m.err
	}

	var batch []event.Event
	for rowSrc.Next() {
		vals, _ := rowSrc.Values()
		id := vals[0].(pgtype.UUID).Bytes
		cid := vals[1].(pgtype.UUID).Bytes
		
		batch = append(batch, event.Event{
			ID:         uuid.UUID(id),
			CampaignID: uuid.UUID(cid),
			Type:       vals[2].(string),
			Payload:    vals[3].([]byte),
			IP:         vals[4].(string),
			UA:         vals[5].(string),
		})
	}
	m.flushes = append(m.flushes, batch)
	return int64(len(batch)), nil
}

func TestProcessor_BufferOverflow(t *testing.T) {
	mock := &MockBatchWriter{}
	// batchSize=1, maxWorkers=1 => buffer size = 1
	p := event.NewProcessor(mock, 1, 1, 1*time.Second, 1*time.Second)

	evt := event.Event{CampaignID: uuid.New(), Type: "click"}

	err := p.Process(evt)
	assert.NoError(t, err)

	// Buffer is now full
	err = p.Process(evt)
	assert.ErrorIs(t, err, event.ErrBufferFull)
}

func TestProcessor_Batching(t *testing.T) {
	mock := &MockBatchWriter{}
	batchSize := 5
	p := event.NewProcessor(mock, batchSize, 1, 1*time.Minute, 1*time.Second)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	for i := 0; i < batchSize; i++ {
		_ = p.Process(event.Event{CampaignID: uuid.New(), Type: "imp"})
	}

	assert.Eventually(t, func() bool {
		mock.mu.Lock()
		defer mock.mu.Unlock()
		return len(mock.flushes) == 1
	}, 500*time.Millisecond, 10*time.Millisecond)

	assert.Equal(t, batchSize, len(mock.flushes[0]))
}

func TestProcessor_Ticker(t *testing.T) {
	mock := &MockBatchWriter{}
	flushInt := 100 * time.Millisecond
	p := event.NewProcessor(mock, 100, 1, flushInt, 1*time.Second)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	_ = p.Process(event.Event{CampaignID: uuid.New(), Type: "click"})

	time.Sleep(flushInt + 50*time.Millisecond)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	require.Len(t, mock.flushes, 1)
	assert.Len(t, mock.flushes[0], 1)
}

func TestProcessor_DrainOnClose(t *testing.T) {
	mock := &MockBatchWriter{}
	p := event.NewProcessor(mock, 100, 1, 1*time.Minute, 1*time.Second)
	
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)

	_ = p.Process(event.Event{CampaignID: uuid.New(), Type: "conv"})
	
	p.Close()
	p.Wait()
	cancel()

	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.Len(t, mock.flushes, 1)
}

func TestProcessor_ClearBatch(t *testing.T) {
	p := &event.Processor{}
	batch := []event.Event{
		{Payload: []byte("data"), IP: "1.2.3.4", UA: "Mozilla"},
		{Payload: []byte("more"), IP: "5.6.7.8", UA: "Safari"},
	}

	p.ClearBatch(&batch)

	assert.Len(t, batch, 0)
	
	reclaimed := batch[:2]
	assert.Nil(t, reclaimed[0].Payload)
	assert.Empty(t, reclaimed[0].IP)
	assert.Empty(t, reclaimed[0].UA)
}
