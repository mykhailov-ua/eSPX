package ingest

import (
	"testing"

	"espx/internal/metrics"

	dto "github.com/prometheus/client_model/go"
)

func TestExportHealthProbeMetrics(t *testing.T) {
	exportHealthProbeMetrics(false, []int32{1, 0, 1})
	if v := gaugeValue(t, metrics.TrackerHealthDegraded); v != 1 {
		t.Fatalf("degraded=%v want 1", v)
	}
	g, err := metrics.TrackerRedisShardHealthy.GetMetricWithLabelValues("1")
	if err != nil {
		t.Fatal(err)
	}
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatal(err)
	}
	if m.GetGauge().GetValue() != 0 {
		t.Fatalf("shard 1 healthy=%v want 0", m.GetGauge().GetValue())
	}
	exportHealthProbeMetrics(true, []int32{1, 1, 1})
	if v := gaugeValue(t, metrics.TrackerHealthDegraded); v != 0 {
		t.Fatalf("degraded=%v want 0", v)
	}
}

func gaugeValue(t *testing.T, g interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}
