-- Add placement_id to impressions, clicks, conversions (M17).

USE ad_event_processor;

ALTER TABLE impressions ADD COLUMN IF NOT EXISTS placement_id String AFTER campaign_id;
ALTER TABLE clicks ADD COLUMN IF NOT EXISTS placement_id String AFTER campaign_id;
ALTER TABLE conversions ADD COLUMN IF NOT EXISTS placement_id String AFTER campaign_id;

-- Update mv_placement_stats_hourly to include clicks and conversions.
-- Since ClickHouse MVs only trigger on source table inserts, we need multiple MVs
-- feeding into a common table if we want to combine data from different tables.

-- 1. Create the destination table for all placement stats.
CREATE TABLE IF NOT EXISTS placement_stats_hourly (
    campaign_id UUID,
    placement_id String,
    hour DateTime,
    spend_micro Int64,
    revenue_micro Int64,
    click_count UInt64,
    conversion_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, placement_id, hour);

-- 2. Create MVs feeding into placement_stats_hourly.

-- Feed from cost_snapshots (spend/revenue)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_placement_stats_money_hourly
TO placement_stats_hourly
AS SELECT
    campaign_id,
    placement_id,
    toStartOfHour(snapshot_hour) AS hour,
    sumIf(amount_usd_micro, line_type = 'spend') AS spend_micro,
    sumIf(amount_usd_micro, line_type = 'revenue') AS revenue_micro,
    0 AS click_count,
    0 AS conversion_count
FROM cost_snapshots
GROUP BY campaign_id, placement_id, hour;

-- Feed from clicks
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_placement_stats_clicks_hourly
TO placement_stats_hourly
AS SELECT
    campaign_id,
    placement_id,
    toStartOfHour(created_at) AS hour,
    0 AS spend_micro,
    0 AS revenue_micro,
    count() AS click_count,
    0 AS conversion_count
FROM clicks
GROUP BY campaign_id, placement_id, hour;

-- Feed from conversions
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_placement_stats_convs_hourly
TO placement_stats_hourly
AS SELECT
    campaign_id,
    placement_id,
    toStartOfHour(created_at) AS hour,
    0 AS spend_micro,
    0 AS revenue_micro,
    0 AS click_count,
    count() AS conversion_count
FROM conversions
GROUP BY campaign_id, placement_id, hour;

-- We can keep mv_placement_stats_hourly as a alias or view if needed for compatibility,
-- but the worker should query placement_stats_hourly.
CREATE OR REPLACE VIEW mv_placement_stats_hourly AS SELECT * FROM placement_stats_hourly;
