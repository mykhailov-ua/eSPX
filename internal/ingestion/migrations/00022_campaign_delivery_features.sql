-- Campaign templates, scheduling, dayparting, brand creatives (A/B landing URLs).

CREATE TABLE campaign_templates (
    id              UUID PRIMARY KEY,
    customer_id     UUID NOT NULL REFERENCES customers(id),
    name            TEXT NOT NULL,
    budget_limit    BIGINT NOT NULL DEFAULT 0,
    pacing_mode     pacing_mode_type NOT NULL DEFAULT 'ASAP',
    daily_budget    BIGINT NOT NULL DEFAULT 0,
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    freq_limit      INTEGER NOT NULL DEFAULT 0,
    freq_window     INTEGER NOT NULL DEFAULT 86400,
    target_countries TEXT[] NOT NULL DEFAULT '{}',
    brand_id        UUID REFERENCES advertiser_brands(id),
    daypart_hours   SMALLINT[] NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_campaign_templates_customer ON campaign_templates(customer_id);

ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS start_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS end_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS daypart_hours SMALLINT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS template_id UUID REFERENCES campaign_templates(id);

CREATE INDEX idx_campaigns_schedule_window ON campaigns(start_at, end_at)
    WHERE status IN ('ACTIVE', 'PAUSED') AND deleted_at IS NULL;

CREATE TABLE brand_creatives (
    id          UUID PRIMARY KEY,
    brand_id    UUID NOT NULL REFERENCES advertiser_brands(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    landing_url TEXT NOT NULL,
    weight      INTEGER NOT NULL DEFAULT 100 CHECK (weight > 0),
    status      TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PAUSED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (brand_id, name)
);

CREATE INDEX idx_brand_creatives_active ON brand_creatives(brand_id) WHERE status = 'ACTIVE';
