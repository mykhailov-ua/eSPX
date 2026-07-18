package billing

import (
	"context"
	"fmt"
	"log/slog"

	"espx/internal/billing/pb"
	notifierpb "espx/internal/notifier/pb"
)

// InvoiceDeliverer sends invoice PDF notifications via notifier gRPC.
type InvoiceDeliverer interface {
	DeliverInvoice(ctx context.Context, customerID, invoiceID, month, currency string, totalMicro int64, pdfURL string) error
}

// NotifierInvoiceDeliverer enqueues invoice delivery through the notifier service.
type NotifierInvoiceDeliverer struct {
	client    notifierpb.NotifierServiceClient
	provider  notifierpb.Provider
	recipient string
	baseURL   string
}

// NewNotifierInvoiceDeliverer constructs a deliverer when notifier is configured.
func NewNotifierInvoiceDeliverer(
	client notifierpb.NotifierServiceClient,
	provider notifierpb.Provider,
	recipient, baseURL string,
) *NotifierInvoiceDeliverer {
	if client == nil || recipient == "" {
		return nil
	}
	return &NotifierInvoiceDeliverer{
		client:    client,
		provider:  provider,
		recipient: recipient,
		baseURL:   baseURL,
	}
}

// DeliverInvoice enqueues a templated notification with a PDF download link.
func (d *NotifierInvoiceDeliverer) DeliverInvoice(
	ctx context.Context,
	customerID, invoiceID, month, currency string,
	totalMicro int64,
	pdfURL string,
) error {
	if d == nil || d.client == nil {
		return fmt.Errorf("notifier deliverer not configured")
	}

	title := fmt.Sprintf("Invoice %s", month)
	_, err := d.client.SendNotification(ctx, &notifierpb.SendNotificationRequest{
		Provider:   d.provider,
		Recipient:  d.recipient,
		Title:      title,
		TemplateId: "invoice_monthly",
		TemplateVars: map[string]string{
			"customer_id":   customerID,
			"invoice_id":    invoiceID,
			"billing_month": month,
			"currency":      currency,
			"total_micro":   fmt.Sprintf("%d", totalMicro),
		},
		AttachmentUrl: pdfURL,
		DedupKey:      fmt.Sprintf("invoice:%s", invoiceID),
	})
	return err
}

// DriftAlerter notifies operators when ledger invariant checks fail.
type DriftAlerter interface {
	AlertLedgerDrift(ctx context.Context, customerID string, err error)
}

// NotifierDriftAlerter sends ledger drift alerts via notifier.
type NotifierDriftAlerter struct {
	deliverer *NotifierInvoiceDeliverer
}

// NewNotifierDriftAlerter wraps a notifier client for drift alerts.
func NewNotifierDriftAlerter(client notifierpb.NotifierServiceClient, provider notifierpb.Provider, recipient string) *NotifierDriftAlerter {
	if client == nil || recipient == "" {
		return nil
	}
	return &NotifierDriftAlerter{
		deliverer: &NotifierInvoiceDeliverer{client: client, provider: provider, recipient: recipient},
	}
}

// AlertLedgerDrift enqueues an ops notification for billing ledger drift.
func (a *NotifierDriftAlerter) AlertLedgerDrift(ctx context.Context, customerID string, driftErr error) {
	if a == nil || a.deliverer == nil || driftErr == nil {
		return
	}
	title := "eSPX: billing ledger drift"
	body := fmt.Sprintf("<b>Ledger invariant failed</b>\nCustomer: %s\nError: %v", customerID, driftErr)
	_, err := a.deliverer.client.SendNotification(ctx, &notifierpb.SendNotificationRequest{
		Provider:  a.deliverer.provider,
		Recipient: a.deliverer.recipient,
		Title:     title,
		Body:      body,
		DedupKey:  fmt.Sprintf("billing:drift:%s", customerID),
	})
	if err != nil {
		slog.Warn("ledger drift alert enqueue failed", "customer_id", customerID, "error", err)
	}
}

// DeliverInvoiceFromProto delivers a generated invoice when a deliverer is configured.
func (s *Service) DeliverInvoice(ctx context.Context, inv *pb.Invoice) error {
	if s == nil || inv == nil || s.deliverer == nil {
		return nil
	}
	month := ""
	if inv.BillingMonth != nil {
		month = inv.BillingMonth.AsTime().UTC().Format("2006-01")
	}
	pdfURL := s.invoicePDFURL(inv.Id)
	return s.deliverer.DeliverInvoice(ctx, inv.CustomerId, inv.Id, month, inv.Currency, inv.TotalMicro, pdfURL)
}

func (s *Service) invoicePDFURL(invoiceID string) string {
	if s == nil || s.invoiceBaseURL == "" {
		return ""
	}
	return s.invoiceBaseURL + "/api/v1/billing/invoices/" + invoiceID + "/pdf"
}
