package dashboard

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildTrafficChart(t *testing.T) {
	now := time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC)
	chart := BuildTrafficChart(now)

	if chart.Type != "line" {
		t.Fatalf("type=%q want line", chart.Type)
	}
	if len(chart.Labels) != 48 || len(chart.Series[0].Values) != 48 {
		t.Fatalf("points=%d/%d want 48", len(chart.Labels), len(chart.Series[0].Values))
	}

	raw := ChartJSON(chart)
	var decoded ChartView
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Title != "Ingest rate" {
		t.Fatalf("title=%q", decoded.Title)
	}
	if decoded.Labels[len(decoded.Labels)-1] != now.Format("15:04:05") {
		t.Fatalf("last label=%q want %q", decoded.Labels[len(decoded.Labels)-1], now.Format("15:04:05"))
	}
}
