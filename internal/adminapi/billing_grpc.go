package adminapi

import (
	"context"
	"time"

	"espx/internal/billing"
	billingpb "espx/internal/billing/pb"

	"github.com/google/uuid"
)

// InvoiceGRPCClient proxies invoice reads to cmd/billing over gRPC.
type InvoiceGRPCClient interface {
	ListInvoices(ctx context.Context, customerID string, limit, offset int32) (*billingpb.ListInvoicesResponse, error)
	GetInvoice(ctx context.Context, invoiceID string) (*billingpb.Invoice, error)
}

// InProcessInvoiceService exposes preview and void using the shared Postgres pool.
type InProcessInvoiceService interface {
	PreviewInvoice(ctx context.Context, customerID uuid.UUID, billingMonth time.Time) (*billing.InvoicePreview, error)
	VoidInvoice(ctx context.Context, invoiceID uuid.UUID) error
}

// InvoiceRetryer re-enqueues invoice delivery via notifier.
type InvoiceRetryer interface {
	RetryInvoiceDelivery(ctx context.Context, invoice *billingpb.Invoice, idempotencyKey string) error
}

// VoidAuditor records void actions in admin audit log.
type VoidAuditor interface {
	AuditInvoiceVoid(ctx context.Context, invoiceID, customerID string) error
}
