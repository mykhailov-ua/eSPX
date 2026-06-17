-- recon_materialized_views.sql: pre-aggregated ClickHouse signals for secondary recon checks.
-- Primary financial reconciliation uses PostgreSQL ledger plus Redis budget:sync keys from Lua.
-- These views detect event loss or duplication between ingestion and settlement without full scans.

USE ad_event_processor;

-- Hourly campaign event counts from impressions; O(1) volume lookup during recon runs.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_campaign_hourly_events
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, hour)
POPULATE
AS SELECT
    campaign_id,
    toStartOfHour(created_at) AS hour,
    count() AS event_count,
    sum(1) AS impression_count,  -- placeholder; in real system would be conditional on table
    sum(1) AS click_count
FROM impressions
GROUP BY campaign_id, hour;

-- Extend with per-event-type MVs or a unified wide table before relying on this in prod recon.
-- Example query:
-- SELECT sum(event_count) FROM mv_campaign_hourly_events
-- WHERE campaign_id = ? AND hour BETWEEN ? AND ?;
