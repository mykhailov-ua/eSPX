package management

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"espx/ui/dashboard"
)

const defaultDashboardFanoutInterval = time.Second

// DefaultDashboardFanoutInterval is the metrics refresh cadence for dashboard fan-out.
func DefaultDashboardFanoutInterval() time.Duration {
	return defaultDashboardFanoutInterval
}

// DashboardBroadcast is a pre-rendered dashboard poll payload shared by all clients.
type DashboardBroadcast struct {
	Version   uint64
	ETag      string
	Snapshot  dashboard.Snapshot
	HTML      []byte
	ChartTrig []byte
}

// DashboardFanout aggregates metrics on a fixed interval and renders poll HTML once per tick.
type DashboardFanout struct {
	interval         time.Duration
	source           dashboardMetricsSource
	live             atomic.Pointer[DashboardBroadcast]
	version          atomic.Uint64
	htmlRenderBuf    bytes.Buffer
	htmlScratch      []byte
	chartTrigScratch []byte
}

// NewDashboardFanout creates a fan-out cache and populates the first broadcast synchronously.
// When source is nil a synthetic metrics source is used (tests and local dev without backends).
func NewDashboardFanout(interval time.Duration, source dashboardMetricsSource) *DashboardFanout {
	if interval <= 0 {
		interval = defaultDashboardFanoutInterval
	}
	if source == nil {
		source = syntheticDashboardSource{}
	}
	f := &DashboardFanout{
		interval: interval,
		source:   source,
	}
	if err := f.tickOnce(time.Now().UTC()); err != nil {
		slog.Error("dashboard fanout initial tick failed", "error", err)
	}
	return f
}

// Start runs the background worker until ctx is cancelled.
func (f *DashboardFanout) Start(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := f.tickOnce(now.UTC()); err != nil {
				slog.Warn("dashboard fanout tick failed", "error", err)
			}
		}
	}
}

// Current returns the latest broadcast. Nil only before the first successful tick.
func (f *DashboardFanout) Current() *DashboardBroadcast {
	return f.live.Load()
}

func (f *DashboardFanout) tickOnce(now time.Time) error {
	snap, err := f.source.Collect(context.Background(), now)
	if err != nil {
		return fmt.Errorf("collect metrics: %w", err)
	}

	html, err := f.renderPollPanelsHTML(snap)
	if err != nil {
		return fmt.Errorf("render poll panels: %w", err)
	}

	chartTrig, err := f.marshalChartTrigger(snap.TrafficChart)
	if err != nil {
		return fmt.Errorf("marshal chart trigger: %w", err)
	}

	v := f.version.Add(1)
	bc := &DashboardBroadcast{
		Version:   v,
		ETag:      fmt.Sprintf(`"dash-%d"`, v),
		Snapshot:  snap,
		HTML:      html,
		ChartTrig: chartTrig,
	}
	f.live.Store(bc)
	return nil
}

func (f *DashboardFanout) renderPollPanelsHTML(snap dashboard.Snapshot) ([]byte, error) {
	f.htmlRenderBuf.Reset()
	if err := dashboard.PollPanels(snap).Render(context.Background(), &f.htmlRenderBuf); err != nil {
		return nil, err
	}
	n := f.htmlRenderBuf.Len()
	if cap(f.htmlScratch) < n {
		f.htmlScratch = make([]byte, n, n*2)
	} else {
		f.htmlScratch = f.htmlScratch[:n]
	}
	copy(f.htmlScratch, f.htmlRenderBuf.Bytes())
	out := make([]byte, n)
	copy(out, f.htmlScratch)
	return out, nil
}

func (f *DashboardFanout) marshalChartTrigger(chart dashboard.ChartView) ([]byte, error) {
	payload := map[string]dashboard.ChartView{
		"dashboardChart": chart,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if cap(f.chartTrigScratch) < len(b) {
		f.chartTrigScratch = make([]byte, len(b), len(b)+len(b)/4)
	}
	f.chartTrigScratch = f.chartTrigScratch[:len(b)]
	copy(f.chartTrigScratch, b)
	out := make([]byte, len(b))
	copy(out, f.chartTrigScratch)
	return out, nil
}

// writeDashboardPoll serves a pre-rendered poll fragment with optional ETag short-circuit.
func writeDashboardPoll(w http.ResponseWriter, r *http.Request, bc *DashboardBroadcast) {
	if bc == nil {
		http.Error(w, "dashboard not ready", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("ETag", bc.ETag)
	if r.Header.Get("If-None-Match") == bc.ETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("HX-Trigger", string(bc.ChartTrig))
	_, _ = w.Write(bc.HTML)
}
