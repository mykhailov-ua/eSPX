# M8 Crypto Gateway Technical Report

This report outlines the design, implementation, and verification of the **M8 — Crypto Gateway** milestone. The implementation adds support for USDT (ERC20/TRC20) cryptocurrency top-ups into the existing payment pipeline with robust signature verification, webhook idempotency, underpay protections, a 14-day hold period, and an automated fraud gate.

---

## 1. Architecture & Design

The Crypto Gateway is built on top of the existing payment outbox-settlement pattern, ensuring high reliability, isolation, and compliance.

```
+------------------+     Webhook     +------------------------+
|  Crypto Network  | --------------> |  Webhook HTTP Handler  |
+------------------+                 +------------------------+
                                                 |
                                                 | ProcessCryptoWebhook
                                                 v
                                     +------------------------+
                                     |    Payment Service     |
                                     +------------------------+
                                                 |
                                                 | Create Crypto Hold (HELD)
                                                 v
                                     +------------------------+
                                     |  payment.crypto_holds  |
                                     +------------------------+
                                                 |
                                                 | 14-day Hold & Fraud Gate
                                                 v
                                     +------------------------+
                                     |   Crypto Hold Worker   |
                                     +------------------------+
                                                 |
                                                 | Enqueue SETTLE_BALANCE
                                                 v
                                     +------------------------+
                                     |     Payment Outbox     |
                                     +------------------------+
                                                 |
                                                 | Outbox Worker
                                                 v
                                     +------------------------+
                                     |   Management Service   |
                                     +------------------------+
                                                 |
                                                 | ApplyPaymentCredit
                                                 v
                                     +------------------------+
                                     |    Customer Balance    |
                                     +------------------------+
```

### 1.1 Components

1. **`CryptoProvider` (`internal/payment/provider_crypto.go`)**:
   - Implements the generic `Provider` interface.
   - Configurable for minimum payment limit (`CryptoMinPaymentMicro`) and confirmation depth (`CryptoConfirmationDepth`).
   - Generates deterministic, unique provider references and checkout URLs.

2. **Database Schema (`internal/payment/migrations/00006_crypto_gateway.sql`)**:
   - Creates the `payment.crypto_holds` table to track crypto top-ups, transaction hashes, hold release times, and statuses (`HELD`, `RELEASED`, `FRAUD_BLOCKED`).
   - Adds indexes on `(status, release_at)` and `customer_id` for efficient polling.

3. **Webhook HTTP Handler (`internal/payment/http_webhook.go`)**:
   - Mounts `/webhooks/crypto` to handle incoming payment notifications.
   - Verifies Stripe-style HMAC-SHA256 signatures using `CryptoWebhookSecret` to prevent forged events.

4. **Webhook Business Logic (`internal/payment/service.go`)**:
   - Deduplicates incoming events using `webhook_events` to guarantee once-and-only-once processing.
   - Rejects underpaid transactions (`amount_micro < intent.AmountMicro`) or those below the configured minimum.
   - Transitions intent status to `PROCESSING` if confirmations are below the threshold, and to `SUCCEEDED` once the depth is met.
   - Creates a hold record with a 14-day release window upon success.

5. **`CryptoHoldWorker` (`internal/payment/crypto_hold_worker.go`)**:
   - A background worker running in `cmd/payment` that polls for eligible holds (`release_at <= now()`).
   - Evaluates the **Fraud Gate**: Blocks release and marks the hold `FRAUD_BLOCKED` if the customer has any open disputes or failed/disputed payment intents.
   - Otherwise, marks the hold `RELEASED` and enqueues a `SETTLE_BALANCE` outbox event.

---

## 2. Verification & Chaos Testing

To guarantee the reliability of the Crypto Gateway under extreme conditions, we implemented comprehensive integration and chaos tests running against real PostgreSQL and Redis containers.

### 2.1 Test Cases

1. **`TestCryptoGateway_EndToEnd`**:
   - Verifies the entire happy path: intent creation, processing pending confirmations, successful confirmation, hold creation, fast-forwarding the hold, hold release by `CryptoHoldWorker`, and balance settlement by `OutboxWorker`.
2. **`TestCryptoGateway_UnderpayRejected`**:
   - Verifies that transactions with insufficient amounts are rejected and marked `FAILED`.
3. **`TestCryptoGateway_FraudGateBlocksRelease`**:
   - Verifies that the fraud gate blocks hold release if the customer has an active dispute.
4. **`TestCryptoGateway_WebhookHTTPHandler`**:
   - Verifies signature verification and payload decoding on the HTTP endpoint.
5. **`TestChaos_CryptoWebhookStormIdempotent`**:
   - Simulates a storm of 50 concurrent duplicate webhooks. Verifies that only one hold is created, and the system processes the webhooks completely idempotently.

### 2.2 Chaos Test Results

The chaos test successfully proved the idempotency of our webhook processing pipeline under high concurrency:

```bash
=== RUN   TestChaos_CryptoWebhookStormIdempotent
2026/07/20 15:27:51 INFO created crypto hold hold_id=019f7f7f-3722-7929-a9b5-ebd406011188 intent_id=019f7f7f-371c-78f3-af6e-bb3eef3e7f23 release_at=2026-08-03T12:27:51.458Z
2026/07/20 15:27:51 INFO crypto webhook event already processed event_id=evt_crypto_storm_659cf710-e8e3-4cc3-88a1-d7d06c1fded9
2026/07/20 15:27:51 INFO crypto webhook event deduplicated by unique constraint event_id=evt_crypto_storm_659cf710-e8e3-4cc3-88a1-d7d06c1fded9
...
    crypto_webhook_chaos_test.go:87: chaos_proof fault=crypto_webhook_storm idempotent=true
--- PASS: TestChaos_CryptoWebhookStormIdempotent (2.48s)
PASS
ok  	espx/internal/payment	13.019s
```

**Proof Line Emitted:**
`chaos_proof fault=crypto_webhook_storm idempotent=true`
