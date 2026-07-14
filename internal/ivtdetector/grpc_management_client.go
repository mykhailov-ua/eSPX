package ivtdetector

import (
	"context"
	"fmt"

	"espx/internal/management/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// GRPCManagementClient enqueues fraud blacklist entries via settlement gRPC BlockIP.
type GRPCManagementClient struct {
	client pb.SettlementServiceClient
	token  string
}

// NewGRPCManagementClient dials management settlement gRPC for internal BlockIP.
func NewGRPCManagementClient(target, token string) (*GRPCManagementClient, *grpc.ClientConn, error) {
	if target == "" {
		return nil, nil, fmt.Errorf("management gRPC target required")
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("management gRPC dial %s: %w", target, err)
	}
	return &GRPCManagementClient{
		client: pb.NewSettlementServiceClient(conn),
		token:  token,
	}, conn, nil
}

// BlockIP enqueues a fraud blacklist entry via internal gRPC.
func (client *GRPCManagementClient) BlockIP(ctx context.Context, ip string) error {
	if client == nil || client.client == nil {
		return fmt.Errorf("management gRPC client: nil receiver")
	}
	if ip == "" {
		return ErrInvalidIP
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-internal-token", client.token)
	_, err := client.client.BlockIP(ctx, &pb.BlockIPRequest{
		Ip:     ip,
		Source: blacklistSourceFraud,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrManagementUnavailable, err)
	}
	return nil
}

// EnqueueMLThreat enqueues an ML threat candidate via internal gRPC.
func (client *GRPCManagementClient) EnqueueMLThreat(ctx context.Context, action string, ip string, campaignID string, score float64, boost int32, ttlSeconds int64) error {
	if client == nil || client.client == nil {
		return fmt.Errorf("management gRPC client: nil receiver")
	}
	if ip == "" {
		return ErrInvalidIP
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-internal-token", client.token)
	_, err := client.client.EnqueueMLThreat(ctx, &pb.EnqueueMLThreatRequest{
		Action:     action,
		Ip:         ip,
		CampaignId: campaignID,
		Score:      score,
		Boost:      boost,
		TtlSeconds: ttlSeconds,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrManagementUnavailable, err)
	}
	return nil
}
