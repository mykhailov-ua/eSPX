-- recon_materialized_views.sql: pre-aggregated ClickHouse signals for secondary recon checks.
-- Primary financial reconciliation uses PostgreSQL ledger plus Redis budget:sync keys from Lua.
-- These views detect event loss or duplication between ingestion and settlement without full scans.

USE ad_event_processor;

-- Hourly impression counts per campaign; O(1) volume lookup during recon runs.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_campaign_hourly_impressions
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, hour)
POPULATE
AS SELECT
    campaign_id,
    toStartOfHour(created_at) AS hour,
    count() AS impression_count
FROM impressions
GROUP BY campaign_id, hour;

-- Hourly click counts per campaign; separate MV keeps SummingMergeTree semantics per event table.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_campaign_hourly_clicks
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, hour)
POPULATE
AS SELECT
    campaign_id,
    toStartOfHour(created_at) AS hour,
    count() AS click_count
FROM clicks
GROUP BY campaign_id, hour;

-- Hourly conversion counts per campaign; mirrors impressions/clicks MV layout.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_campaign_hourly_conversions
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, hour)
POPULATE
AS SELECT
    campaign_id,
    toStartOfHour(created_at) AS hour,
    count() AS conversion_count
FROM conversions
GROUP BY campaign_id, hour;
