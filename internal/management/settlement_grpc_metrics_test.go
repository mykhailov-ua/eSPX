package management

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSettlementGRPCMetricsInterceptor_recordsSuccess(t *testing.T) {
	method := "/test.SettlementService/ApplyPaymentCredit/success"
	interceptor := SettlementGRPCMetricsInterceptor()

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method},
		func(ctx context.Context, req any) (any, error) {
			time.Sleep(2 * time.Millisecond)
			return "ok", nil
		},
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, testutil.CollectAndCount(settlementGRPCRequestDuration), 1)
}

func TestSettlementGRPCMetricsInterceptor_recordsError(t *testing.T) {
	method := "/test.SettlementService/ApplyPaymentRefund/error"
	interceptor := SettlementGRPCMetricsInterceptor()
	wantErr := status.Error(codes.NotFound, "customer not found")

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method},
		func(ctx context.Context, req any) (any, error) {
			return nil, wantErr
		},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, wantErr))
	require.GreaterOrEqual(t, testutil.CollectAndCount(settlementGRPCRequestDuration), 1)
	require.Equal(t, float64(1), testutil.ToFloat64(settlementGRPCErrorsTotal.WithLabelValues(method, codes.NotFound.String())))
}
