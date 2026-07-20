-- Margin Guard policies and activity log (M17).

CREATE TABLE IF NOT EXISTS margin_guard_policies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    min_clicks INT NOT NULL DEFAULT 50,
    roi_floor_pct FLOAT NOT NULL DEFAULT -30.0,
    zero_conv_streak INT NOT NULL DEFAULT 100,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_margin_guard_policies_campaign_id ON margin_guard_policies(campaign_id);

CREATE TABLE IF NOT EXISTS margin_guard_activity (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id UUID NOT NULL REFERENCES margin_guard_policies(id) ON DELETE CASCADE,
    campaign_id UUID NOT NULL,
    placement_id TEXT NOT NULL,
    action TEXT NOT NULL, -- 'pause', 'resume', 'alert'
    reason TEXT NOT NULL,
    metrics JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_margin_guard_activity_campaign_id ON margin_guard_activity(campaign_id);
CREATE INDEX IF NOT EXISTS idx_margin_guard_activity_created_at ON margin_guard_activity(created_at);

-- Add to outbox_events types documentation if any, or just use 'PAUSE_PLACEMENT'
