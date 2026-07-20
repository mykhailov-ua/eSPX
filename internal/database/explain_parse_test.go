package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExplainPlan_outboxIndexScan(t *testing.T) {
	raw := `Limit  (cost=0.42..8.44 rows=1 width=100) (actual time=0.015..0.016 rows=1 loops=1)
  Buffers: shared hit=4
  ->  Index Scan using idx_outbox_pending on outbox_events  (cost=0.42..8.44 rows=1 width=100) (actual time=0.014..0.014 rows=1 loops=1)
        Index Cond: (status = 'PENDING'::text)
        Buffers: shared hit=4
Planning Time: 0.082 ms
Execution Time: 0.035 ms`
	plan := ParseExplainPlan(raw)
	require.Equal(t, 0.035, plan.ExecutionTimeMS)
	require.GreaterOrEqual(t, len(plan.Nodes), 2)
	findings := AnalyzeExplainPlan("test", plan, true, 500)
	assert.Empty(t, findings)
}

func TestAnalyzeExplainPlan_seqScanLargeWithFilter(t *testing.T) {
	raw := `Seq Scan on campaigns  (cost=0.00..1250.00 rows=5000 width=200) (actual time=0.01..2.5 rows=8000 loops=1)
  Filter: (status = 'ACTIVE'::text)
  Rows Removed by Filter: 12000
  Buffers: shared hit=800
Execution Time: 3.5 ms`
	plan := ParseExplainPlan(raw)
	require.Equal(t, int64(12000), plan.Nodes[0].RowsRemoved)
	findings := AnalyzeExplainPlan("active_campaigns", plan, false, 500)
	require.NotEmpty(t, findings)
	assert.Equal(t, "warn", findings[0].Severity)
}

func TestAnalyzeExplainPlan_nestedRowsRemovedAttachedToScan(t *testing.T) {
	raw := `HashAggregate  (cost=1..2 rows=1 width=8) (actual time=5..5 rows=1 loops=1)
  ->  Seq Scan on balance_ledger  (cost=0..1500 rows=12500 width=24) (actual time=0..4 rows=12500 loops=1)
        Filter: (type = 'FEE'::ledger_type)
        Rows Removed by Filter: 37500
        Buffers: shared hit=667
Execution Time: 5.5 ms`
	plan := ParseExplainPlan(raw)
	var scan *ExplainNode
	for i := range plan.Nodes {
		if plan.Nodes[i].Relation == "balance_ledger" {
			scan = &plan.Nodes[i]
			break
		}
	}
	require.NotNil(t, scan)
	require.Equal(t, int64(37500), scan.RowsRemoved)
	findings := AnalyzeExplainPlan("ledger_window", plan, false, 500)
	require.NotEmpty(t, findings)
}
