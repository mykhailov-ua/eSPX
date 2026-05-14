-- +goose Up
-- +goose StatementBegin
CREATE TYPE ledger_type AS ENUM ('TOPUP', 'FREEZE', 'RELEASE', 'FEE', 'REFUND');

CREATE TABLE balance_ledger (
    id BIGSERIAL PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(id),
    campaign_id UUID REFERENCES campaigns(id),
    amount NUMERIC(20, 2) NOT NULL,
    type ledger_type NOT NULL,
    idempotency_hash TEXT UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ledger_customer ON balance_ledger(customer_id);
CREATE INDEX idx_ledger_campaign ON balance_ledger(campaign_id);

CREATE TABLE campaign_status_history (
    id BIGSERIAL PRIMARY KEY,
    campaign_id UUID NOT NULL REFERENCES campaigns(id),
    old_status campaign_status_type,
    new_status campaign_status_type NOT NULL,
    reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_status_history_campaign ON campaign_status_history(campaign_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE campaign_status_history;
DROP TABLE balance_ledger;
DROP TYPE ledger_type;
-- +goose StatementEnd
