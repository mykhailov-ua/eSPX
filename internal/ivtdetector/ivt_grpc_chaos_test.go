package ivtdetector

import (
	"context"
	"net"
	"testing"

	"espx/internal/management/pb"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type recordingBlockIPServer struct {
	pb.UnimplementedSettlementServiceServer
	calls []string
}

func (s *recordingBlockIPServer) BlockIP(_ context.Context, req *pb.BlockIPRequest) (*pb.BlockIPResponse, error) {
	s.calls = append(s.calls, req.GetIp())
	return &pb.BlockIPResponse{Enqueued: true}, nil
}

// TestChaos_IVTGRPCBlockIP verifies the gRPC management client enqueues BlockIP without HTTP admin key.
func TestChaos_IVTGRPCBlockIP(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	rec := &recordingBlockIPServer{}
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
	require.NoError(t, client.BlockIP(ctx, "203.0.113.9"))
	require.Equal(t, []string{"203.0.113.9"}, rec.calls)

	t.Logf("chaos_proof fault=ivt_grpc_block_ip subsystem=ivt ip=203.0.113.9 transport=grpc")
}
