-- +goose Up
-- +goose StatementBegin
CREATE TABLE regions (
    code SMALLINT PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE outbox_region_delivery (
    outbox_event_id BIGINT NOT NULL REFERENCES outbox_events(id) ON DELETE CASCADE,
    region_code SMALLINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    processing_started_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (outbox_event_id, region_code)
);

CREATE INDEX idx_outbox_region_pending
    ON outbox_region_delivery (region_code, status, created_at)
    WHERE status IN ('PENDING', 'PROCESSING');

CREATE TABLE region_apply_idempotency (
    region_code SMALLINT NOT NULL,
    outbox_event_id BIGINT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (region_code, outbox_event_id)
);

CREATE OR REPLACE FUNCTION fanout_outbox_region_delivery()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO outbox_region_delivery (outbox_event_id, region_code)
    SELECT NEW.id, r.code
    FROM regions r
    WHERE r.active = TRUE
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_region_fanout
    AFTER INSERT ON outbox_events
    FOR EACH ROW
    EXECUTE FUNCTION fanout_outbox_region_delivery();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS outbox_region_fanout ON outbox_events;
DROP FUNCTION IF EXISTS fanout_outbox_region_delivery();
DROP TABLE IF EXISTS region_apply_idempotency;
DROP TABLE IF EXISTS outbox_region_delivery;
DROP TABLE IF EXISTS regions;
-- +goose StatementEnd
