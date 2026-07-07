-- IAB supply chain: sellers.json, ads.txt entries, and per-campaign schain nodes (max 10 hops).

-- +goose Up
-- +goose StatementBegin
CREATE TABLE sellers (
    id          BIGSERIAL PRIMARY KEY,
    seller_id   TEXT NOT NULL,
    domain      TEXT NOT NULL,
    seller_type TEXT NOT NULL CHECK (seller_type IN ('PUBLISHER', 'INTERMEDIARY', 'BOTH')),
    name        TEXT NOT NULL DEFAULT '',
    is_confidential BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (seller_id)
);

CREATE INDEX idx_sellers_domain ON sellers (domain);

CREATE TABLE ads_txt_entries (
    id                   BIGSERIAL PRIMARY KEY,
    domain               TEXT NOT NULL,
    publisher_account_id TEXT NOT NULL,
    relationship         TEXT NOT NULL CHECK (relationship IN ('DIRECT', 'RESELLER')),
    cert_authority_id    TEXT NOT NULL DEFAULT '',
    sort_order           INT NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ads_txt_entries_sort ON ads_txt_entries (sort_order, id);

ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS supply_chain_nodes JSONB NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(supply_chain_nodes) = 'array'
               AND jsonb_array_length(supply_chain_nodes) <= 10);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE campaigns DROP COLUMN IF EXISTS supply_chain_nodes;
DROP TABLE IF EXISTS ads_txt_entries;
DROP TABLE IF EXISTS sellers;
-- +goose StatementEnd
