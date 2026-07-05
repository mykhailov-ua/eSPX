# Payment Postgres Isolation — Technical Report

Date: 2026-07-04  
Status: Implemented

## Executive summary

Payment checkout state (`payment` schema) now runs on a dedicated Postgres instance `db-payment` in Docker Compose. The `cmd/payment` binary connects via `PAYMENT_DB_DSN`, applies embedded migrations on startup with idempotent `payment.schema_migrations` tracking, and no longer shares the core `db` container with ads ledger, auth, billing, or notifier data.

## Motivation

Schema isolation via `payment.*` qualified tables already prevented cross-schema FK coupling, but a single Postgres instance still shared:

- Connection pool contention with tracker/management workloads
- Backup/restore blast radius (PCI-adjacent webhook payloads in `payment.webhook_events`)
- Compliance boundary (separate credentials and volume for payment data)

Container separation is the logical next step after schema separation.

## Topology

```mermaid
flowchart LR
  subgraph core_db["db :5430"]
    ads[public / ads]
    auth[auth]
    billing[billing]
    notifier[notifier]
  end

  subgraph pay_db["db-payment :5431"]
    payment[payment schema]
  end

  M[management] --> core_db
  PAY[payment] --> pay_db
  PAY -->|settlement gRPC| M
```

## Changes

| Area | Change |
| :--- | :--- |
| `docker-compose.yml` | New `db-payment` service, volume `db_payment_data`, payment `depends_on: db-payment` |
| `.env.example` | `PAYMENT_DB_*` credentials, `PAYMENT_DB_PORT=5431`, `PAYMENT_DB_DSN` |
| `internal/config/env.go` | `PaymentDBDSN`; falls back to `DB_DSN` when unset |
| `cmd/payment/main.go` | Connects to `PaymentDBDSN`, runs `payment.ApplyMigrations` on boot |
| `internal/payment/migrate.go` | Embedded goose migrations + `payment.schema_migrations` ledger |
| `internal/database/goose_migrate.go` | Shared goose Up parser for tests and other services |
| `scripts/dev/dev_stack.sh` | `db-payment` in `infra` and `full` profiles |

## Configuration

```bash
PAYMENT_DB_PORT=5431
PAYMENT_DB_NAME=espx_payment
PAYMENT_DB_USER=espx_payment_user
PAYMENT_DB_PASSWORD=...
PAYMENT_DB_DSN=postgres://espx_payment_user:...@127.0.0.1:5431/espx_payment?sslmode=disable
```

Backward compatibility: omit `PAYMENT_DB_DSN` to keep payment on `DB_DSN` (single-DB local dev).

## Migration from shared database

For existing stacks with `payment` schema on `db`:

```bash
pg_dump -h 127.0.0.1 -p 5430 -U ad_event_processor_user -d ad_event_processor -n payment -Fc -f payment_schema.dump
pg_restore -h 127.0.0.1 -p 5431 -U espx_payment_user -d espx_payment --no-owner payment_schema.dump
```

Then set `PAYMENT_DB_DSN` and restart `payment`. Fresh compose stacks auto-migrate on first payment boot.

## Out of scope

- Billing/notifier DB split (remain on `db`)
- Cross-DB foreign keys (never existed; `customer_id` is logical UUID only)
- Automated pg_dump replication between instances
