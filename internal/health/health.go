// Package health provides liveness (/healthz) and readiness (/readyz) handlers with cached dependency probes.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

const bodyOK = "OK"

// Liveness tracks /healthz hits without I/O on the request path.
type Liveness struct {
	hits atomic.Uint64
}

// Hit records one liveness probe (tracker hot path).
func (l *Liveness) Hit() {
	if l != nil {
		l.hits.Add(1)
	}
}

// Hits returns the total /healthz invocations.
func (l *Liveness) Hits() uint64 {
	if l == nil {
		return 0
	}
	return l.hits.Load()
}

// ServeHealthz answers liveness probes with no dependency I/O.
func ServeHealthz(l *Liveness, w http.ResponseWriter, _ *http.Request) {
	if l != nil {
		l.Hit()
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(bodyOK))
}

// ReadinessProbe caches dependency health for /readyz (p99 < 10 ms).
type ReadinessProbe struct {
	ready atomic.Int32
}

// SetReady updates the cached readiness flag.
func (p *ReadinessProbe) SetReady(ok bool) {
	if p == nil {
		return
	}
	if ok {
		p.ready.Store(1)
	} else {
		p.ready.Store(0)
	}
}

// Ready reports whether the process may receive traffic.
func (p *ReadinessProbe) Ready() bool {
	return p != nil && p.ready.Load() == 1
}

// ServeReadyz serves the readiness endpoint from cached atomics only.
func (p *ReadinessProbe) ServeReadyz(w http.ResponseWriter, _ *http.Request) {
	if p.Ready() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(bodyOK))
		return
	}
	http.Error(w, "not ready", http.StatusServiceUnavailable)
}

// StartBackground periodically runs check and publishes readiness.
func (p *ReadinessProbe) StartBackground(ctx context.Context, interval time.Duration, check func(context.Context) bool) {
	if p == nil || check == nil {
		return
	}
	p.SetReady(true)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				probeCtx, cancel := context.WithTimeout(ctx, interval)
				p.SetReady(check(probeCtx))
				cancel()
			}
		}
	}()
}

// Register mounts GET /healthz and optional GET /readyz on mux.
func Register(mux *http.ServeMux, live *Liveness, ready *ReadinessProbe) {
	if mux == nil {
		return
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ServeHealthz(live, w, r)
	})
	if ready != nil {
		mux.HandleFunc("GET /readyz", ready.ServeReadyz)
	}
}
