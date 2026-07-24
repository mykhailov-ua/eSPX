package ingestion

import (
	"context"
	"testing"
	"time"

	"espx/internal/campaignmodel"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type traceFilter struct {
	name  string
	trace *[]string
	fail  error
}

func (f *traceFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	*f.trace = append(*f.trace, f.name)
	return f.fail
}

// TestFilterEngine_ProductionOrder asserts filters run in registration order until first error (M6-08).
func TestFilterEngine_ProductionOrder(t *testing.T) {
	var order []string
	emergency := &traceFilter{name: "emergency", trace: &order}
	geo := &traceFilter{name: "geo", trace: &order}
	schedule := &traceFilter{name: "schedule", trace: &order}
	unified := &traceFilter{name: "unified", trace: &order, fail: ErrRateLimitExceeded}

	engine := NewFilterEngine(time.Second, emergency, geo, schedule, unified)
	evt := &campaignmodel.Event{CampaignID: uuid.New()}

	err := engine.Check(context.Background(), evt)
	require.ErrorIs(t, err, ErrRateLimitExceeded)
	assert.Equal(t, []string{"emergency", "geo", "schedule", "unified"}, order)
}

// TestFilterEngine_DeadlineShortCircuit stops the chain when the monotonic deadline expires (M6-08).
func TestFilterEngine_DeadlineShortCircuit(t *testing.T) {
	var order []string
	engine := NewFilterEngine(5*time.Millisecond,
		&traceFilter{name: "fast", trace: &order},
		&slowFilter{delay: 20 * time.Millisecond},
		&traceFilter{name: "never", trace: &order, fail: ErrBudgetExhausted},
	)
	evt := &campaignmodel.Event{CampaignID: uuid.New()}
	err := engine.Check(context.Background(), evt)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, []string{"fast"}, order)
}
