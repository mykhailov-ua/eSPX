-- OpenRTB PMP deals catalog (M3.1 control plane).

-- +goose Up
-- +goose StatementBegin
CREATE TABLE rtb_deals (
    id          BIGSERIAL PRIMARY KEY,
    deal_id     TEXT NOT NULL,
    floor_micro BIGINT NOT NULL DEFAULT 0 CHECK (floor_micro >= 0),
    geo_mask    BIGINT NOT NULL DEFAULT 0,
    cat_mask    BIGINT NOT NULL DEFAULT 0,
    pacing      SMALLINT NOT NULL DEFAULT 1 CHECK (pacing IN (1, 2)),
    customer_id UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (deal_id)
);

CREATE INDEX idx_rtb_deals_customer_id ON rtb_deals (customer_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS rtb_deals;
-- +goose StatementEnd
