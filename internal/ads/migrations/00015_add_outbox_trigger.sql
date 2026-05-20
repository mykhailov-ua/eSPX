-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_outbox_event()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('outbox_channel', CAST(NEW.id AS text));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER outbox_event_trigger
AFTER INSERT ON outbox_events
FOR EACH ROW
EXECUTE FUNCTION notify_outbox_event();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS outbox_event_trigger ON outbox_events;
DROP FUNCTION IF EXISTS notify_outbox_event();
-- +goose StatementEnd
