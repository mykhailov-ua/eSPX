package management

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"espx/internal/metrics"
)

// ErrMgmtPgGateRejected is returned when a LOW-tier worker must yield to HIGH-tier traffic.
var ErrMgmtPgGateRejected = errors.New("mgmt pg gate rejected")

const mgmtPgReserve = 1

// MgmtPgGate limits concurrent management Postgres usage with HIGH/LOW priority (SEM-P3).
type MgmtPgGate struct {
	sem      chan struct{}
	capacity int
	lowSlots chan struct{}
	inFlight atomic.Int32
}

// NewMgmtPgGate sizes the gate from DB_TRACKER_MAX_CONNS minus one reserve slot for probes.
func NewMgmtPgGate(maxConns int) *MgmtPgGate {
	capacity := maxConns - mgmtPgReserve
	if capacity < 2 {
		capacity = 2
	}
	lowCap := capacity - 1
	if lowCap < 1 {
		lowCap = 1
	}
	return &MgmtPgGate{
		sem:      make(chan struct{}, capacity),
		capacity: capacity,
		lowSlots: make(chan struct{}, lowCap),
	}
}

// AcquireHigh blocks until a slot is available for HTTP, outbox, or drain work.
func (g *MgmtPgGate) AcquireHigh(ctx context.Context) error {
	if g == nil {
		return nil
	}
	start := time.Now()
	select {
	case g.sem <- struct{}{}:
		if wait := time.Since(start); wait > 0 {
			metrics.MgmtPgGateAcquireWaitSeconds.WithLabelValues("high").Observe(wait.Seconds())
		}
		g.inFlight.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseHigh returns a HIGH-tier slot.
func (g *MgmtPgGate) ReleaseHigh() {
	if g == nil {
		return
	}
	g.inFlight.Add(-1)
	<-g.sem
}

// AcquireLow blocks for a background slot or rejects when LOW budget is exhausted.
func (g *MgmtPgGate) AcquireLow(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case g.lowSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	default:
		metrics.MgmtPgGateRejectedTotal.WithLabelValues("low").Inc()
		return ErrMgmtPgGateRejected
	}

	start := time.Now()
	select {
	case g.sem <- struct{}{}:
		if wait := time.Since(start); wait > 0 {
			metrics.MgmtPgGateAcquireWaitSeconds.WithLabelValues("low").Observe(wait.Seconds())
		}
		g.inFlight.Add(1)
		return nil
	case <-ctx.Done():
		<-g.lowSlots
		return ctx.Err()
	}
}

// ReleaseLow returns a LOW-tier slot.
func (g *MgmtPgGate) ReleaseLow() {
	if g == nil {
		return
	}
	g.inFlight.Add(-1)
	<-g.sem
	<-g.lowSlots
}

// InFlight returns holders that have acquired but not released.
func (g *MgmtPgGate) InFlight() int {
	if g == nil {
		return 0
	}
	return int(g.inFlight.Load())
}

func (s *Service) withPgHigh(ctx context.Context, fn func(context.Context) error) error {
	if s == nil || s.pgGate == nil {
		return fn(ctx)
	}
	if err := s.pgGate.AcquireHigh(ctx); err != nil {
		return err
	}
	defer s.pgGate.ReleaseHigh()
	return fn(ctx)
}

func (s *Service) withPgLow(ctx context.Context, fn func(context.Context) error) error {
	if s == nil || s.pgGate == nil {
		return fn(ctx)
	}
	if err := s.pgGate.AcquireLow(ctx); err != nil {
		return err
	}
	defer s.pgGate.ReleaseLow()
	return fn(ctx)
}
