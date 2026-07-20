package ivtdetector

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"espx/internal/fraudscoring"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type shadowBatchConn struct {
	driver.Conn
	prepareCalls int
	appendCalls  int
	sendCalls    int
}

func (c *shadowBatchConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.prepareCalls++
	if !strings.Contains(query, "ml_shadow_scores") {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	return &shadowBatchRecorder{parent: c}, nil
}

type shadowBatchRecorder struct {
	driver.Batch
	parent *shadowBatchConn
}

func (b *shadowBatchRecorder) Append(v ...any) error {
	b.parent.appendCalls++
	return nil
}

func (b *shadowBatchRecorder) Send() error {
	b.parent.sendCalls++
	return nil
}

func TestInsertShadowScores_UsesSinglePrepareBatch(t *testing.T) {
	t.Parallel()

	conn := &shadowBatchConn{}
	rule := &fraudScoringRule{
		writeConn: conn,
		scorer:    &mockScorer{scores: []float64{0.1, 0.9}},
	}

	rows := []fraudscoring.FeatureRow{
		{IPAddress: "1.2.3.4"},
		{IPAddress: "5.6.7.8"},
	}
	require.NoError(t, rule.insertShadowScores(context.Background(), rows, []float64{0.1, 0.9}))

	assert.Equal(t, 1, conn.prepareCalls)
	assert.Equal(t, 2, conn.appendCalls)
	assert.Equal(t, 1, conn.sendCalls)
}
