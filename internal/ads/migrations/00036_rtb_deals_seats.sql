-- M3.6: enforce minimum one buyer seat per PMP deal; document bidfloor micro-units column.

-- +goose Up
-- +goose StatementBegin
ALTER TABLE rtb_deals
    ADD COLUMN IF NOT EXISTS seats INT NOT NULL DEFAULT 1 CHECK (seats >= 1);

COMMENT ON COLUMN rtb_deals.floor_micro IS 'OpenRTB bidfloor in micro-units (int64 CPM)';
COMMENT ON COLUMN rtb_deals.seats IS 'Minimum buyer seats (wseat) required for the deal; must be >= 1';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE rtb_deals DROP COLUMN IF EXISTS seats;
-- +goose StatementEnd
