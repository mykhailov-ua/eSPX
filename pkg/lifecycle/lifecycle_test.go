package lifecycle_test

import (
	"net/http"
	"testing"
	"time"

	"espx/pkg/lifecycle"
)

func TestShutdownHTTPServerNil(t *testing.T) {
	if err := lifecycle.ShutdownHTTPServer(nil, time.Second); err != nil {
		t.Fatalf("nil server: %v", err)
	}
}

func TestMetricsServerShutdownNil(t *testing.T) {
	var m *lifecycle.MetricsServer
	if err := m.Shutdown(time.Second); err != nil {
		t.Fatalf("nil metrics: %v", err)
	}
}

func TestShutdownHTTPServerAlreadyClosed(t *testing.T) {
	srv := &http.Server{Addr: ":0"}
	if err := lifecycle.ShutdownHTTPServer(srv, 100*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
