-- M7 R3: per-campaign auction reserve price in micro-units (second-price floor).

-- +goose Up
-- +goose StatementBegin
ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS reserve_micro BIGINT NOT NULL DEFAULT 0 CHECK (reserve_micro >= 0);

COMMENT ON COLUMN campaigns.reserve_micro IS 'Minimum clearing price in micro-units for RTB auction (applyReserve)';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE campaigns DROP COLUMN IF EXISTS reserve_micro;
-- +goose StatementEnd
