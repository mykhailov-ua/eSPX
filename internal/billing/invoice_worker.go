package billing

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// InvoiceWorker runs monthly invoice generation on the 1st at 00:15 UTC.
type InvoiceWorker struct {
	service *Service
}

// NewInvoiceWorker constructs the monthly billing cron worker.
func NewInvoiceWorker(service *Service) *InvoiceWorker {
	return &InvoiceWorker{service: service}
}

// Start runs the scheduler until ctx is cancelled.
func (w *InvoiceWorker) Start(ctx context.Context) {
	if w == nil || w.service == nil {
		return
	}

	for {
		wait := durationUntilNextInvoiceRun(time.Now().UTC())
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			w.runMonth(ctx, previousBillingMonth(time.Now().UTC()))
		}
	}
}

func durationUntilNextInvoiceRun(now time.Time) time.Duration {
	next := time.Date(now.Year(), now.Month(), 1, 0, 15, 0, 0, time.UTC)
	if !now.Before(next) {
		next = next.AddDate(0, 1, 0)
	}
	return next.Sub(now)
}

func previousBillingMonth(now time.Time) time.Time {
	u := now.UTC()
	return time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -1, 0)
}

func (w *InvoiceWorker) runMonth(ctx context.Context, month time.Time) {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	acquired, err := w.service.tryInvoiceCronLock(opCtx)
	if err != nil {
		return
	}
	if !acquired {
		return
	}
	defer w.service.releaseInvoiceCronLock(context.Background())

	const pageSize int32 = 200
	var offset int32
	for {
		ids, err := w.service.ListCustomerIDs(opCtx, pageSize, offset)
		if err != nil {
			return
		}
		if len(ids) == 0 {
			break
		}
		for _, customerID := range ids {
			inv, genErr := w.service.GenerateInvoice(opCtx, customerID, month)
			if genErr != nil {
				if errors.Is(genErr, ErrNoSpend) {
					continue
				}
				continue
			}
			_ = w.service.DeliverInvoice(opCtx, inv)
		}
		if len(ids) < int(pageSize) {
			break
		}
		offset += pageSize
	}
}

// RunInvoiceMonthForTest exposes a single monthly sweep for chaos tests.
func (w *InvoiceWorker) RunInvoiceMonthForTest(ctx context.Context, month time.Time) {
	if w != nil {
		w.runMonth(ctx, month)
	}
}

const invoiceCronLockKey = int64(0x657370785f696e76)

func (service *Service) tryInvoiceCronLock(ctx context.Context) (bool, error) {
	var ok bool
	err := service.pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, invoiceCronLockKey).Scan(&ok)
	return ok, err
}

func (service *Service) releaseInvoiceCronLock(ctx context.Context) {
	_, _ = service.pool.Exec(ctx, `SELECT pg_advisory_unlock($1)`, invoiceCronLockKey)
}

// GenerateInvoiceForCustomers runs idempotent invoice generation for explicit customers (tests).
func (service *Service) GenerateInvoiceForCustomers(ctx context.Context, customerIDs []uuid.UUID, month time.Time) {
	for _, id := range customerIDs {
		inv, err := service.GenerateInvoice(ctx, id, month)
		if err == nil && inv != nil {
			_ = service.DeliverInvoice(ctx, inv)
		}
	}
}
