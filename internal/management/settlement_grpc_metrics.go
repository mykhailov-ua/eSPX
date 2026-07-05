package management

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	settlementGRPCRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "settlement_grpc_request_duration_seconds",
		Help:    "Settlement gRPC unary request latency in seconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"method"})

	settlementGRPCErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "settlement_grpc_errors_total",
		Help: "Settlement gRPC unary requests that returned an error",
	}, []string{"method", "code"})
)

// SettlementGRPCMetricsInterceptor records latency and error codes for payment settlement RPCs.
func SettlementGRPCMetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		settlementGRPCRequestDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		if err != nil {
			settlementGRPCErrorsTotal.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
		}
		return resp, err
	}
}
