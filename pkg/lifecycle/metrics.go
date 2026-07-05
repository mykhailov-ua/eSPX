package lifecycle

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsServer serves /metrics on addr with production HTTP timeouts.
type MetricsServer struct {
	Server *http.Server
}

// StartMetrics listens on addr (host:port or :port) and serves Prometheus metrics.
func StartMetrics(addr string) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
		}
	}()
	return &MetricsServer{Server: srv}
}

// Shutdown drains the metrics HTTP listener.
func (m *MetricsServer) Shutdown(timeout time.Duration) error {
	if m == nil || m.Server == nil {
		return nil
	}
	return ShutdownHTTPServer(m.Server, timeout)
}
