package payment

import (
	"context"
	"fmt"

	"espx/internal/config"
	notifierpb "espx/internal/notifier/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NotifierClient calls the notifier gRPC service to enqueue operator alerts from payment.
type NotifierClient struct {
	conn   *grpc.ClientConn
	client notifierpb.NotifierServiceClient
}

// NewNotifierClient dials notifier when ops alerts are enabled and a recipient is configured.
func NewNotifierClient(cfg *config.Config) (*NotifierClient, error) {
	if cfg == nil || !cfg.OpsAlertsEnabled() {
		return nil, nil
	}

	target := cfg.Notifier.ServerHost + ":" + cfg.Notifier.Port
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("notifier gRPC dial %s: %w", target, err)
	}

	return &NotifierClient{
		conn:   conn,
		client: notifierpb.NewNotifierServiceClient(conn),
	}, nil
}

// Close releases the gRPC connection on payment shutdown.
func (client *NotifierClient) Close() error {
	if client == nil || client.conn == nil {
		return nil
	}
	return client.conn.Close()
}

// SendNotification enqueues an alert for asynchronous delivery by the notifier worker.
func (client *NotifierClient) SendNotification(ctx context.Context, req *notifierpb.SendNotificationRequest) (*notifierpb.SendNotificationResponse, error) {
	if client == nil || client.client == nil {
		return nil, fmt.Errorf("notifier client not configured")
	}
	return client.client.SendNotification(ctx, req)
}
