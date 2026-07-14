package ivtdetector

import (
	"context"
	"net"
	"testing"

	"espx/internal/management/pb"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type recordingMLThreatServer struct {
	pb.UnimplementedSettlementServiceServer
	requests []*pb.EnqueueMLThreatRequest
}

func (s *recordingMLThreatServer) EnqueueMLThreat(_ context.Context, req *pb.EnqueueMLThreatRequest) (*pb.EnqueueMLThreatResponse, error) {
	s.requests = append(s.requests, req)
	return &pb.EnqueueMLThreatResponse{Enqueued: true}, nil
}

// TestChaos_MLGRPCEnqueueThreat verifies ML boost enqueue over settlement gRPC.
func TestChaos_MLGRPCEnqueueThreat(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	rec := &recordingMLThreatServer{}
	pb.RegisterSettlementServiceServer(srv, rec)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	client, conn, err := NewGRPCManagementClient(lis.Addr().String(), "test-token")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	require.NoError(t, client.EnqueueMLThreat(ctx, "boost", "203.0.113.60", "00000000-0000-0000-0000-000000000001", 45, 45, 300))
	require.Len(t, rec.requests, 1)
	require.Equal(t, "boost", rec.requests[0].GetAction())
	require.Equal(t, "203.0.113.60", rec.requests[0].GetIp())

	logChaosProof(t, "ml_grpc_block_ip", map[string]string{
		"subsystem": "ml_analytics",
		"transport": "grpc",
		"action":    "boost",
	})
}
