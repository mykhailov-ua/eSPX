package management

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/ui/dashboard"
)

func TestDashboardFanoutInitialBroadcast(t *testing.T) {
	f := NewDashboardFanout(time.Hour, nil)

	bc := f.Current()
	if bc == nil {
		t.Fatal("expected initial broadcast")
	}
	if bc.Version != 1 {
		t.Fatalf("version=%d want 1", bc.Version)
	}
	if len(bc.HTML) == 0 {
		t.Fatal("expected pre-rendered HTML")
	}
	if len(bc.ChartTrig) == 0 {
		t.Fatal("expected chart trigger JSON")
	}
	if !strings.Contains(string(bc.HTML), "Requests / sec") {
		t.Fatalf("html missing stat label: %q", bc.HTML[:min(120, len(bc.HTML))])
	}
	if bc.Snapshot.RequestsPerSec == "" {
		t.Fatal("expected snapshot on broadcast")
	}
}

func TestDashboardFanoutTickIncrementsVersion(t *testing.T) {
	f := NewDashboardFanout(time.Hour, nil)

	before := f.Current().Version
	if err := f.tickOnce(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	after := f.Current().Version
	if after != before+1 {
		t.Fatalf("version %d -> %d want +1", before, after)
	}
}

func TestDashboardFanoutWorkerUpdates(t *testing.T) {
	f := NewDashboardFanout(20*time.Millisecond, nil)
	start := f.Current().Version

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Start(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if f.Current().Version > start {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("version still %d after worker run", start)
}

func TestWriteDashboardPollServesHTML(t *testing.T) {
	f := NewDashboardFanout(time.Hour, nil)
	bc := f.Current()

	req := httptest.NewRequest(http.MethodGet, "/admin/dashboard/poll", nil)
	rec := httptest.NewRecorder()
	writeDashboardPoll(rec, req, bc)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content-type=%q", ct)
	}
	if got := rec.Header().Get("HX-Trigger"); got == "" {
		t.Fatal("missing HX-Trigger")
	}
	if !strings.Contains(rec.Body.String(), "Requests / sec") {
		t.Fatal("body missing stat panel")
	}
	if rec.Header().Get("ETag") != bc.ETag {
		t.Fatalf("etag=%q want %q", rec.Header().Get("ETag"), bc.ETag)
	}
}

func TestWriteDashboardPollNotModified(t *testing.T) {
	f := NewDashboardFanout(time.Hour, nil)
	bc := f.Current()

	req := httptest.NewRequest(http.MethodGet, "/admin/dashboard/poll", nil)
	req.Header.Set("If-None-Match", bc.ETag)
	rec := httptest.NewRecorder()
	writeDashboardPoll(rec, req, bc)

	if rec.Code != http.StatusNotModified {
		t.Fatalf("status=%d want 304", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body len=%d want 0", rec.Body.Len())
	}
}

func TestDashboardFanoutConcurrent5000Readers(t *testing.T) {
	f := NewDashboardFanout(time.Hour, nil)
	bc := f.Current()

	const clients = 5000
	var ok atomic.Uint64

	var wg sync.WaitGroup
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			got := f.Current()
			if got != nil && got.Version == bc.Version && len(got.HTML) == len(bc.HTML) {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	if n := ok.Load(); n != clients {
		t.Fatalf("consistent reads=%d want %d", n, clients)
	}
}

func TestDashboardPollHandlerConcurrent5000(t *testing.T) {
	f := NewDashboardFanout(time.Hour, nil)
	bc := f.Current()

	const clients = 5000
	var wg sync.WaitGroup
	wg.Add(clients)

	errCh := make(chan error, clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/admin/dashboard/poll", nil)
			rec := httptest.NewRecorder()
			writeDashboardPoll(rec, req, bc)
			if rec.Code != http.StatusOK {
				errCh <- io.ErrUnexpectedEOF
				return
			}
			body, err := io.ReadAll(rec.Body)
			if err != nil {
				errCh <- err
				return
			}
			if len(body) != len(bc.HTML) {
				errCh <- io.ErrShortBuffer
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDashboardPollPerRequestRenderBaseline(t *testing.T) {
	now := time.Now().UTC()
	snap := buildSyntheticSnapshot(now)

	f := &DashboardFanout{source: syntheticDashboardSource{}}
	html, err := f.renderPollPanelsHTML(snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(html) == 0 {
		t.Fatal("empty render")
	}
	_, err = f.marshalChartTrigger(snap.TrafficChart)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDashboardFanoutTick(b *testing.B) {
	f := NewDashboardFanout(time.Hour, nil)
	now := time.Now().UTC()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.tickOnce(now); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDashboardFanoutServe(b *testing.B) {
	f := NewDashboardFanout(time.Hour, nil)
	bc := f.Current()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/poll", nil)
		writeDashboardPoll(rec, req, bc)
	}
}

func BenchmarkDashboardFanoutConcurrent5000(b *testing.B) {
	f := NewDashboardFanout(time.Hour, nil)
	bc := f.Current()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(5000)
		for j := 0; j < 5000; j++ {
			go func() {
				defer wg.Done()
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/poll", nil)
				writeDashboardPoll(rec, req, bc)
			}()
		}
		wg.Wait()
	}
}

func BenchmarkDashboardPollPerRequestRender(b *testing.B) {
	now := time.Now().UTC()
	f := &DashboardFanout{source: syntheticDashboardSource{}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap := buildSyntheticSnapshot(now)
		if _, err := f.renderPollPanelsHTML(snap); err != nil {
			b.Fatal(err)
		}
		if _, err := f.marshalChartTrigger(snap.TrafficChart); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDashboardPollPerRequestRenderConcurrent5000(b *testing.B) {
	now := time.Now().UTC()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(5000)
		for j := 0; j < 5000; j++ {
			go func() {
				defer wg.Done()
				f := &DashboardFanout{source: syntheticDashboardSource{}}
				snap := buildSyntheticSnapshot(now)
				if _, err := f.renderPollPanelsHTML(snap); err != nil {
					return
				}
				_, _ = f.marshalChartTrigger(snap.TrafficChart)
			}()
		}
		wg.Wait()
	}
}

// Guard compile-time coupling between fan-out snapshot and dashboard view model.
var _ = dashboard.Snapshot{}
