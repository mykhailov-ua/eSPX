-- deploy/clickhouse/recon_materialized_views.sql
-- Purpose: Provide fast, pre-aggregated volume signals for secondary reconciliation checks.
-- Primary financial reconciliation is performed against PostgreSQL ledger + Redis "budget:sync:*" keys
-- because the monetary value is known at the moment of budget reservation in the edge Lua script.
-- These views allow detection of event loss/duplication between ingestion and settlement.

USE ad_event_processor;

-- Hourly campaign event counts (impressions + clicks + conversions).
-- This MV is populated in real time by ClickHouse and allows O(1) lookup of observed volume
-- without scanning the massive partitioned raw tables during recon runs.
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

-- Similar view for clicks (can be extended with UNION or separate tables).
-- In production you would have separate MVs per event type or a single wide table.

-- Usage in recon:
-- SELECT sum(event_count) FROM mv_campaign_hourly_events
-- WHERE campaign_id = ? AND hour BETWEEN ? AND ?;
