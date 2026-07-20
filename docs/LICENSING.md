# Product Licensing (on-prem)

Product license for sales: license + consulting + deployment. Buyer runs eSPX on their hardware; vendor issues signed entitlements via a **non-blocking** license server.

Tenant plans (Basic / Pro / Enterprise): [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md). Unified doc: [MANAGEMENT.md](./MANAGEMENT.md) §18–19.

---

## 1. Delivery Model

| Role | Where | Responsibility |
| :--- | :--- | :--- |
| **Vendor** | `license.espx.io` (`cmd/license-server`) | issue, renew, revoke JWT |
| **Buyer** | their DC / VPS | operations, data, network |
| **eSPX stack** | buyer hardware | tracker, redis, PG, management |

License server has **no access** to buyer Postgres, Redis streams, or event payloads. Activate/heartbeat metadata only.

---

## 2. Non-Blocking Design

Core rule: **no customer binary performs vendor network I/O on the hot path or blocks cold-path request handlers waiting for the license server.**

### 2.1 Isolation Layers

| Layer | Network to vendor | When |
| :--- | :--- | :--- |
| **Hot path** (`/track`) | Never | Reads registry snapshot only |
| **Cold path** (HTTP admin) | Never per request | Reads `license_status` PG / atomic in-memory pointer |
| **Background** (`LicenseWatcher`) | Optional heartbeat | Scheduled goroutine; timeout + circuit breaker |

### 2.2 Failure Behavior

| Scenario | Behavior |
| :--- | :--- |
| License server down (online mode) | Keep **last-known-good** JWT on disk; continue until `EXPIRED` |
| Heartbeat timeout (5s) | Log + metric `license_refresh_failed`; no snapshot change |
| Invalid signature on new JWT | Reject update; keep previous snapshot |
| `valid_until` passed | `GRACE` — ingest continues; admin warning header |
| Grace ended | `EXPIRED` — `filterRejectLicenseExpired` on track |
| Revoke (CRL / 403 heartbeat) | `REVOKED` on next successful refresh cycle |

Watcher refresh must not add > 500ms to admin HTTP p99 (verify and network off request path).

### 2.3 Topology

```text
[Vendor]  license-server                 [Customer DC]
              |                                |
         issue / renew                  management
         signed JWT          <--------   LicenseWatcher (async)
              |                                |
              |                          verify local (Ed25519)
              |                                |
              |                          license.jwt (disk cache)
              |                                |
              |                          Redis entitlement:deployment:{id}
              |                                |
              |                          tracker registry (0 network)
```

---

## 3. Delivery Modes

| Mode | Env | Network |
| :--- | :--- | :--- |
| **file** | `ESPX_LICENSE_PATH=/etc/espx/license.jwt` | None (air-gap) |
| **online** | `ESPX_LICENSE_SERVER`, `ESPX_LICENSE_KEY` | Scheduled heartbeat only |

```text
ESPX_LICENSE_MODE=file|online
ESPX_LICENSE_PATH=/etc/espx/license.jwt
ESPX_LICENSE_SERVER=https://license.example.com
ESPX_LICENSE_KEY=...
ESPX_LICENSE_REFRESH_INTERVAL=24h
ESPX_LICENSE_HEARTBEAT_TIMEOUT=5s
ESPX_LICENSE_TELEMETRY=0|1
```

Same JWT format and embedded vendor public key in binaries for both modes.

---

## 4. License JWT

Ed25519 or PASETO v4 public. Private key — license server only. Public key — `//go:embed` in `management` and `tracker`.

The deployment license JWT is signed using Ed25519 or PASETO v4. The private key resides exclusively on the vendor license server, while the public key is embedded (`//go:embed`) inside `management` and `tracker` binaries. Claims specify: `iss`, `sub` (license UUID), `kid`, `deployment_id`, `customer_name`, `plan` (`starter` | `growth` | `enterprise`), `valid_from`, `valid_until`, `grace_days`, `limits` (`max_rps`, `max_requests_per_day`, `max_active_campaigns`, `max_regions`, `max_tenants`), `features` (`rtb_live`, `ml_fraud_boost`, `multi_region`, `slot_migration`), hardware `bind` mode, and `support_tier`.

`plan` here is the **deployment ceiling**: `starter` | `growth` | `enterprise` (not tenant `basic` | `pro`).

Compatible with [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md): one `Entitlements` type; `Effective(deployment, customer)` in code.

### 4.1 Deployment Binding

| mode | Behavior |
| :--- | :--- |
| `none` | hardware move without vendor approval |
| `soft` | fingerprint on first activate; change via vendor portal |
| `hard` | fingerprint in JWT at issue (bare metal) |

---

## 5. License Server (vendor)

Separate binary; not in customer compose.

```text
cmd/license-server/
internal/licensing/          # shared verify + types (customer repo)
```

Vendor Postgres: `licenses`, `deployments`, `renewal_events`, `revocations`.

| Endpoint | Purpose |
| :--- | :--- |
| `POST /v1/licenses/issue` | vendor admin: new contract |
| `POST /v1/licenses/renew` | extend `valid_until` (payment webhook) |
| `POST /v1/licenses/revoke` | revocation |
| `POST /v1/activate` | `license_key` + `deployment_id` → JWT |
| `POST /v1/heartbeat` | metadata → new JWT or **304 Not Modified** |
| `GET /v1/revocations` | signed CRL; management cache TTL 1h |

Payment webhook → `renew` → buyer gets new JWT on next heartbeat or manual file delivery.

### 5.1 Heartbeat Client (customer management)

- HTTP client with **5s timeout**, max 2 retries, exponential backoff.
- Circuit breaker: after N failures, skip heartbeats for 1h (still use cached JWT).
- Atomic write: `license.jwt.tmp` → rename `license.jwt`.
- On success: verify → update PG `license_status` → Redis → registry notify.

---

## 6. Deployment License Plans

Ceiling for the entire installation. Tenant subscriptions cannot exceed these without license upgrade.

| Plan | Typical buyer | max_tenants | max_rps | max_requests_per_day (total) | max_regions |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **starter** | Single arbitrage team | 5 | 25,000 | 2,500,000 | 1 |
| **growth** | Agency / hub | 50 | 100,000 | 100,000,000 | 2 |
| **enterprise** | Large holding | unlimited | 500,000+ | contract | contract |

---

## 7. Customer Runtime

| Component | Role |
| :--- | :--- |
| `management` | `LicenseWatcher`; verify on startup; PG `license_status` + Redis snapshot |
| `tracker` | Registry reads snapshot; no vendor network |
| `processor` | Optional: pause settlement on `EXPIRED` (config) |
| `espx-install` | `license install`, `license activate`, `license status` |

### 7.1 States

| State | Condition | Hot path | Cold path |
| :--- | :--- | :--- | :--- |
| `ACTIVE` | sig OK, `now < valid_until` | full per plan | — |
| `GRACE` | within `grace_days` after expiry | ingest OK | `X-License-State: grace` |
| `EXPIRED` | grace over | `filterRejectLicenseExpired` | new campaigns 403 |
| `REVOKED` | CRL / 403 | block on next snapshot update | alert |

Renewal in grace: new JWT → watcher updates snapshot → registry reload without tracker restart.

---

## 8. Heartbeat Privacy

Telemetry heartbeats (when `ESPX_LICENSE_TELEMETRY=1` is explicitly opted in) transmit only aggregate operational metadata: `license_id`, `deployment_id`, binary software versions (`management`, `tracker`), system `uptime_seconds`, and aggregate 24-hour event counts. Transmitting user IP addresses, click IDs, financial ledger entries, or campaign payloads is strictly forbidden.

---

## 9. Customer PG Mirror

The `license_status` table in PostgreSQL acts as the local system-of-record mirror for active entitlements. Schema fields: `deployment_id` (Primary Key), `license_id`, `plan_code`, `valid_until` (TIMESTAMPTZ), `state`, `entitlements_json` (JSONB), `last_verified_at`, and `last_refresh_error`.

API: `GET /api/v1/license/status` — `LicenseStatusDTO` in MANAGEMENT.md §19.8.

---

## 10. Unified Entitlements

The `internal/licensing` package exports unified entitlement merging logic (`Effective(deployment, customer)`). It merges deployment ceilings with tenant subscription parameters, enforcing `min(deployment, customer)` for quantitative limits and boolean `AND` for feature flags.

| Source | Redis key |
| :--- | :--- |
| License JWT | `entitlement:deployment:{deployment_id}` |
| Subscription | `entitlement:customer:{customer_id}` |

Hot path uses `Effective()` per campaign's customer.

---

## 11. Security

1. Signature — term cannot be extended without private key.
2. `kid` — vendor key rotation; old JWTs valid until expiry.
3. CRL cache in management, TTL 1h.
4. No code obfuscation; crypto only.

---

## 12. Prohibitions

1. HTTP to license server on every `/track`.
2. Sync license verify in HTTP handler.
3. Vendor private key in customer compose or git.
4. Inbound vendor connection to customer DC.
5. Mixing vendor license fee and tenant ad spend in one table.

---

## 13. Related Documents

- [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md) — Basic / Pro / Enterprise tenants
- [MANAGEMENT.md](./MANAGEMENT.md) — §18–21
- [PROPOSALS.md](./PROPOSALS.md) — **PROPOSAL:** hybrid volume licensing (optional)
- [MILESTONE.md](./MILESTONE.md) — Milestone 6 (LIC-*, SUB-*)
