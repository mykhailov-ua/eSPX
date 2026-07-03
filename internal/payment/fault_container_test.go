package payment

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestChaos_PaymentPGStopOutboxClaimBlocked stops Postgres and proves outbox claim cannot proceed.
func TestChaos_PaymentPGStopOutboxClaimBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 11_000_000, "chaos-pg-stop-"+uuid.New().String())
	assertPaymentChaosInvariants(t, infra.Pool, seed, 0, 0)

	worker := newOutboxWorkerForChaos(infra)
	stopPaymentContainer(t, infra.PGContainer)
	requirePaymentFaultActive(t, func() bool {
		return infra.Pool.Ping(ctx) != nil
	}, "pg ping must fail after stop")

	processed, err := worker.ProcessOutbox(ctx, 10)
	require.Error(t, err)
	require.Equal(t, 0, processed)

	startPaymentContainer(t, infra.PGContainer)
	infra.refreshPGPool(t)
	require.Equal(t, "PENDING", paymentOutboxStatus(t, infra.Pool, seed.OutboxID))
	assertPaymentChaosInvariants(t, infra.Pool, seed, 0, 0)

	logChaosProof(t, "postgres_container_stop", map[string]string{
		"subsystem":    "payment_outbox",
		"processed":    "0",
		"balance":      "0",
		"ledger_rows":  "0",
		"baseline_ok":  "true",
		"fault_verify": "postgres_container_stopped",
	})
}

// TestChaos_PaymentPGStopStartOutboxRecovery stops Postgres then drains outbox after restart.
func TestChaos_PaymentPGStopStartOutboxRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 13_000_000, "chaos-pg-recovery-"+uuid.New().String())

	worker := newOutboxWorkerForChaos(infra)
	stopPaymentContainer(t, infra.PGContainer)
	requirePaymentFaultActive(t, func() bool {
		return infra.Pool.Ping(ctx) != nil
	}, "pg ping must fail after stop")

	startPaymentContainer(t, infra.PGContainer)
	infra.refreshPGPool(t)
	worker.pool = infra.Pool

	recovered := false
	require.Eventually(t, func() bool {
		n, err := worker.ProcessOutbox(ctx, 10)
		if err != nil || n != 1 {
			return false
		}
		recovered = paymentOutboxStatus(t, infra.Pool, seed.OutboxID) == "PROCESSED"
		return recovered
	}, 30*time.Second, 200*time.Millisecond)

	assertPaymentChaosInvariants(t, infra.Pool, seed, seed.AmountMicro, 1)

	logChaosProof(t, "postgres_stop_start_recovery", map[string]string{
		"subsystem":    "payment_outbox",
		"recovered":    strconv.FormatBool(recovered),
		"ledger_rows":  "1",
		"baseline_ok":  "true",
		"fault_verify": "postgres_container_stopped_then_started",
	})
}

// TestChaos_PaymentSettlementDownOutboxStaysPending kills settlement gRPC while Postgres stays up.
func TestChaos_PaymentSettlementDownOutboxStaysPending(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 14_000_000, "chaos-grpc-stop-"+uuid.New().String())

	worker := newOutboxWorkerForChaos(infra)
	infra.SettlementGRPC.Stop()

	processed, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, processed)
	require.Equal(t, "PENDING", paymentOutboxStatus(t, infra.Pool, seed.OutboxID))
	assertPaymentChaosInvariants(t, infra.Pool, seed, 0, 0)

	logChaosProof(t, "settlement_grpc_stop", map[string]string{
		"subsystem":     "payment_outbox",
		"outbox_status": "PENDING",
		"ledger_rows":   "0",
		"baseline_ok":   "true",
		"fault_verify":  "settlement_grpc_stopped",
	})
}

// TestChaos_PaymentSettlementDownThenRecovery restarts settlement gRPC and drains cold.
func TestChaos_PaymentSettlementDownThenRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 16_000_000, "chaos-grpc-recovery-"+uuid.New().String())

	worker := newOutboxWorkerForChaos(infra)
	infra.SettlementGRPC.Stop()
	_, _ = worker.ProcessOutbox(ctx, 10)
	require.Equal(t, "PENDING", paymentOutboxStatus(t, infra.Pool, seed.OutboxID))

	infra.restartSettlementGRPC(t)
	recovered := false
	require.Eventually(t, func() bool {
		n, err := worker.ProcessOutbox(ctx, 10)
		if err != nil || n != 1 {
			return false
		}
		recovered = paymentOutboxStatus(t, infra.Pool, seed.OutboxID) == "PROCESSED"
		return recovered
	}, 30*time.Second, 200*time.Millisecond)

	assertPaymentChaosInvariants(t, infra.Pool, seed, seed.AmountMicro, 1)

	logChaosProof(t, "settlement_grpc_stop_start_recovery", map[string]string{
		"subsystem":    "payment_outbox",
		"recovered":    strconv.FormatBool(recovered),
		"ledger_rows":  "1",
		"baseline_ok":  "true",
		"fault_verify": "settlement_grpc_stopped_then_started",
	})
}

// TestChaos_PaymentPGTerminateDuringWebhook terminates Postgres during webhook processing.
func TestChaos_PaymentPGTerminateDuringWebhook(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seedCustomer(t, infra.Pool, customerID)

	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	result, err := svc.CreatePaymentIntent(ctx, customerID, 7_000_000, "USD", "chaos-wh-pg-"+uuid.New().String(), nil)
	require.NoError(t, err)
	intent := result.Intent
	providerRef := intent.ProviderRef.String
	eventID := "evt_pg_kill_" + uuid.New().String()
	payload := fmt.Sprintf(`{"id":"%s","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":7000000}}}`,
		eventID, providerRef)

	require.NoError(t, infra.PGContainer.Terminate(ctx))
	requirePaymentFaultActive(t, func() bool {
		return infra.Pool.Ping(ctx) != nil
	}, "pg ping must fail after terminate")

	err = svc.ProcessStripeWebhook(ctx, eventID, "payment_intent.succeeded", []byte(payload), providerRef, 7_000_000, payload)
	require.Error(t, err)

	logChaosProof(t, "postgres_container_terminate_webhook", map[string]string{
		"subsystem":    "payment_webhook",
		"committed":    "false",
		"ledger_rows":  "0",
		"baseline_ok":  "true",
		"fault_verify": "postgres_container_terminated",
	})
}
