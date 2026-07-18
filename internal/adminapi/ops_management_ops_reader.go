package adminapi

import (
	"context"
	"io"
)

// ManagementOpsReader is the management.Service surface used by ops JSON handlers.
type ManagementOpsReader interface {
	GetIncidentSnapshot(ctx context.Context) (IncidentSnapshotDTO, error)
	ListOutboxEvents(ctx context.Context, status, eventType, cursor string, limit int32) (OutboxListResult, error)
	ListDLQEntries(ctx context.Context, cursor string, limit int) (FanOutResult[DLQEntryDTO], error)
	EnqueueDLQRetry(ctx context.Context, payload DLQRetryPayload, idempotencyKey string) error
	GetShardHealthFanOut(ctx context.Context) (ShardHealthAPIResponse, error)
	ExportAuditCSV(ctx context.Context, cursor string, w io.Writer) (AuditExportResult, error)
	LookupLedgerIDForPaymentIntent(ctx context.Context, intentID string) (string, error)
	ListReconRuns(ctx context.Context, service string, limit, offset int32) ([]ReconRunDTO, int64, error)
}
