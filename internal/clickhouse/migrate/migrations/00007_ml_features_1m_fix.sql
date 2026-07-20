-- M-DB-CH-2: partition ml_features_1m, replace uniqExact MVs with uniqCombined.

USE ad_event_processor;

DROP VIEW IF EXISTS mv_ml_features_1m_impressions;
DROP VIEW IF EXISTS mv_ml_features_1m_clicks;

CREATE TABLE IF NOT EXISTS ml_features_1m_partitioned (
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
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, ip_address, campaign_id);

INSERT INTO ml_features_1m_partitioned
SELECT * FROM ml_features_1m;

DROP TABLE IF EXISTS ml_features_1m;

RENAME TABLE ml_features_1m_partitioned TO ml_features_1m;

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
    uniqCombined(ip_address) AS unique_users,
    uniqCombined(user_agent) AS unique_uas
FROM impressions
GROUP BY window_start, ip_address, campaign_id;

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
    uniqCombined(ip_address) AS unique_users,
    uniqCombined(user_agent) AS unique_uas
FROM clicks
GROUP BY window_start, ip_address, campaign_id;
