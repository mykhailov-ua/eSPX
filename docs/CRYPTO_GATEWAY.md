# Crypto Gateway: BTC / ETH / USDT

This document describes integrating cryptocurrency acceptance into the existing payment pipeline. The current release implements Stripe only.

---

## 1. Separation of Responsibilities

*   **`payment` service.** Creates transactions (intents), handles webhooks, and enqueues work in `outbox`. Cryptocurrency providers are implemented here.
*   **`management` service.** Performs final crediting to `balance_ledger`. Works independently of payment method.
*   **`billing` service.** Builds reports from ledger entries. Does not talk to payment providers directly.

---

## 2. Implemented Patterns

1.  **Provider abstraction.** The `Provider` interface allows new payment methods without changing transaction state logic.
2.  **`amount_micro` model.** All amounts convert to micro-units (10⁶) of the system currency (default USD). For crypto, conversion happens at transaction confirmation.
3.  **Idempotency.**
    *   Transaction uniqueness by `idempotency_key`.
    *   Webhook deduplication by provider event ID.
    *   Ledger credit deduplication by transaction ID.
4.  **Outbox.** Successful payment events flow from the payment service to management asynchronously.

---

## 3. Implementation Options

### Option A: External Gateway (Recommended)
Use provider APIs (Coinbase Commerce, BitPay, etc.).
*   **Flow.** Create an order via provider API → redirect the user to the payment page → receive a confirmation webhook.
*   **Benefits.** The provider handles FX rates and blockchain confirmation checks.

### Option B: Direct On-Chain Monitoring
*   **Flow.** Generate a unique deposit address → monitor network transactions (via nodes or indexers) → credit after N confirmations.
*   **Risks.** You must handle exchange-rate calculation and reorg protection yourself.

---

## 4. Change Matrix

| Layer | Integration Impact |
| :--- | :--- |
| **Billing** | No changes required. |
| **Settlement** | No changes required. |
| **Amount Conversion** | Requires a satoshi/wei to micro-unit converter. |
| **Webhooks** | Requires a new route for crypto provider notifications. |
| **ReconService** | Requires reconciliation of on-chain amounts against expected transaction amounts. |

---

## 5. Confirmation Requirements

Credits must occur only after a safety threshold:
*   **USDT (Stablecoins).** Minimum 12 blocks (for Ethereum).
*   **BTC.** Minimum 3–6 blocks.

The exchange rate is fixed in transaction metadata at the moment of on-chain confirmation.
