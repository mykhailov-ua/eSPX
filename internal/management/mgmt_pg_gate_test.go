package management

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMgmtPgGate_LowRejectedWhenBudgetExhausted(t *testing.T) {
	gate := NewMgmtPgGate(3) // capacity=2, lowSlots=1
	ctx := context.Background()

	require.NoError(t, gate.AcquireLow(ctx))
	require.ErrorIs(t, gate.AcquireLow(ctx), ErrMgmtPgGateRejected)
	gate.ReleaseLow()
}

func TestMgmtPgGate_HighUsesReservedSlot(t *testing.T) {
	gate := NewMgmtPgGate(3)
	ctx := context.Background()

	require.NoError(t, gate.AcquireLow(ctx))
	require.NoError(t, gate.AcquireHigh(ctx))
	gate.ReleaseHigh()
	gate.ReleaseLow()
}
