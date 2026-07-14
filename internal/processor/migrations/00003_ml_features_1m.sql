-- ClickHouse migration: ml_features_1m materialized view and backing table, and ml_shadow_scores table.
-- Applied idempotently on processor startup.

USE ad_event_processor;

-- backing table for ml_features_1m
CREATE TABLE IF NOT EXISTS ml_features_1m (
    window_start DateTime,
    ip_address String,
    campaign_id UUID,
    events UInt64,
    clicks UInt64,
    spend_micro Int64,
    budget_limit_micro Int64,
    unique_users UInt64,
    unique_uas UInt64
) ENGINE = SummingMergeTree()
ORDER BY (window_start, ip_address, campaign_id);

-- materialized view for impressions
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ml_features_1m_impressions
TO ml_features_1m
AS SELECT
    toStartOfMinute(created_at) AS window_start,
    ip_address,
    campaign_id,
    count() AS events,
    toUInt64(0) AS clicks,
    toInt64(0) AS spend_micro,
    toInt64(0) AS budget_limit_micro,
    uniqExact(ip_address) AS unique_users,
    uniqExact(user_agent) AS unique_uas
FROM impressions
GROUP BY window_start, ip_address, campaign_id;

-- materialized view for clicks
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ml_features_1m_clicks
TO ml_features_1m
AS SELECT
    toStartOfMinute(created_at) AS window_start,
    ip_address,
    campaign_id,
    count() AS events,
    count() AS clicks,
    toInt64(0) AS spend_micro,
    toInt64(0) AS budget_limit_micro,
    uniqExact(ip_address) AS unique_users,
    uniqExact(user_agent) AS unique_uas
FROM clicks
GROUP BY window_start, ip_address, campaign_id;

-- shadow scores table for ML shadow scoring
CREATE TABLE IF NOT EXISTS ml_shadow_scores (
    ip_address String,
    score Float64,
    model_name LowCardinality(String),
    created_at DateTime64(3, 'UTC')
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (model_name, created_at, ip_address)
TTL toDateTime(created_at) + INTERVAL 90 DAY;
