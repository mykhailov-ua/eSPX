-- +goose Up
-- +goose StatementBegin
CREATE TABLE notifier.templates (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  body       TEXT NOT NULL,
  vars       TEXT[] NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE notifier.notifications
  ADD COLUMN IF NOT EXISTS template_id TEXT REFERENCES notifier.templates(id),
  ADD COLUMN IF NOT EXISTS template_vars JSONB,
  ADD COLUMN IF NOT EXISTS attachment_url TEXT;

INSERT INTO notifier.templates (id, name, body, vars)
VALUES (
  'invoice_monthly',
  'Monthly invoice',
  '<b>Invoice {{billing_month}}</b>\nCustomer: {{customer_id}}\nTotal: {{total_micro}} {{currency}}\n<a href="{{attachment_url}}">Download PDF</a>',
  ARRAY['customer_id', 'invoice_id', 'billing_month', 'currency', 'total_micro', 'attachment_url']
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO notifier.templates (id, name, body, vars)
VALUES (
  'ops_alert',
  'Operations alert',
  '<b>{{title}}</b>\n{{body}}',
  ARRAY['title', 'body']
)
ON CONFLICT (id) DO NOTHING;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE notifier.notifications
  DROP COLUMN IF EXISTS attachment_url,
  DROP COLUMN IF EXISTS template_vars,
  DROP COLUMN IF EXISTS template_id;

DROP TABLE IF EXISTS notifier.templates;
-- +goose StatementEnd
