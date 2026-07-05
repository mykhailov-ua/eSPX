package management

import (
	"context"
	"fmt"

	"espx/internal/config"
	notifierpb "espx/internal/notifier/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NotifierClient calls the notifier gRPC service to enqueue operator alerts.
type NotifierClient struct {
	conn   *grpc.ClientConn
	client notifierpb.NotifierServiceClient
}

// NewNotifierClient dials notifier when ops alerts or the Alertmanager webhook adapter are enabled.
func NewNotifierClient(cfg *config.Config) (*NotifierClient, error) {
	if cfg == nil || !cfg.NotifierDialEnabled() {
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

// Close releases the gRPC connection on management shutdown.
func (client *NotifierClient) Close() error {
	if client == nil || client.conn == nil {
		return nil
	}
	return client.conn.Close()
}

// SendNotification enqueues an alert for asynchronous delivery by the notifier worker.
func (client *NotifierClient) SendNotification(ctx context.Context, provider notifierpb.Provider, recipient, title, body string) (*notifierpb.SendNotificationResponse, error) {
	if client == nil || client.client == nil {
		return nil, fmt.Errorf("notifier client not configured")
	}
	return client.client.SendNotification(ctx, &notifierpb.SendNotificationRequest{
		Provider:  provider,
		Recipient: recipient,
		Title:     title,
		Body:      body,
	})
}

// SendNotificationBatch enqueues multiple alerts in one RPC.
func (client *NotifierClient) SendNotificationBatch(ctx context.Context, notifications []*notifierpb.SendNotificationRequest) (*notifierpb.SendNotificationBatchResponse, error) {
	if client == nil || client.client == nil {
		return nil, fmt.Errorf("notifier client not configured")
	}
	return client.client.SendNotificationBatch(ctx, &notifierpb.SendNotificationBatchRequest{
		Notifications: notifications,
	})
}

// SendBroadcastNotification enqueues a multi-channel fan-out alert.
func (client *NotifierClient) SendBroadcastNotification(
	ctx context.Context,
	provider notifierpb.Provider,
	recipient, title, body string,
	broadcastProviders []notifierpb.Provider,
) (*notifierpb.SendNotificationResponse, error) {
	if client == nil || client.client == nil {
		return nil, fmt.Errorf("notifier client not configured")
	}
	return client.client.SendNotification(ctx, &notifierpb.SendNotificationRequest{
		Provider:           provider,
		Recipient:          recipient,
		Title:              title,
		Body:               body,
		DeliveryMode:       notifierpb.DeliveryMode_DELIVERY_MODE_BROADCAST,
		BroadcastProviders: broadcastProviders,
	})
}
