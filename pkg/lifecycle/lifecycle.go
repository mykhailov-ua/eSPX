// Package lifecycle provides shared graceful-shutdown primitives for long-running binaries.
package lifecycle

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"espx/internal/config"

	google_grpc "google.golang.org/grpc"
)

// Timeouts bounds ingress stop and worker-drain phases during shutdown.
type Timeouts struct {
	Shutdown time.Duration
	Wait     time.Duration
}

// TimeoutsFromConfig reads lifecycle durations from the main service config.
func TimeoutsFromConfig(cfg *config.Config) Timeouts {
	return Timeouts{
		Shutdown: time.Duration(cfg.Lifecycle.ShutdownTimeoutMs) * time.Millisecond,
		Wait:     time.Duration(cfg.Lifecycle.WaitTimeoutMs) * time.Millisecond,
	}
}

// TimeoutsFromEnv reads SHUTDOWN_TIMEOUT_MS and WAIT_TIMEOUT_MS without loading full Config.
func TimeoutsFromEnv() Timeouts {
	return Timeouts{
		Shutdown: config.LifecycleShutdownTimeout(),
		Wait:     config.LifecycleWaitTimeout(),
	}
}

// NotifyContext returns a context cancelled on SIGINT or SIGTERM.
func NotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

// WaitSignal blocks until SIGINT or SIGTERM and returns the received signal.
func WaitSignal() os.Signal {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	sig := <-stop
	signal.Stop(stop)
	return sig
}

// ShutdownHTTPServer drains in-flight HTTP requests before closing the listener.
func ShutdownHTTPServer(srv *http.Server, timeout time.Duration) error {
	if srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return srv.Shutdown(ctx)
}

// ShutdownGRPC stops accepting RPCs, waits for in-flight handlers, then force-stops on timeout.
func ShutdownGRPC(srv *google_grpc.Server, timeout time.Duration) {
	if srv == nil {
		return
	}
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-stopped:
		slog.Info("gRPC server stopped cleanly")
	case <-ctx.Done():
		slog.Warn("gRPC graceful shutdown timed out, force stopping")
		srv.Stop()
	}
}

// Wait blocks until fn completes or the wait timeout expires.
func Wait(timeout time.Duration, fn func()) error {
	if fn == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
