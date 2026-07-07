-- ClickHouse query plans for GET /api/v1/campaigns/{id}/stats hourly aggregates.
-- Run: clickhouse-client --multiquery < scripts/sql/campaign_stats_ch_explain.sql
-- Requires mv_campaign_hourly_* (internal/processor/migrations/00001_campaign_hourly_mvs.sql).

USE ad_event_processor;

CREATE TABLE IF NOT EXISTS tmp_campaign_hourly_seed AS mv_campaign_hourly_impressions
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, hour);

TRUNCATE TABLE IF EXISTS tmp_campaign_hourly_clicks_seed;
TRUNCATE TABLE IF EXISTS tmp_campaign_hourly_conv_seed;
TRUNCATE TABLE tmp_campaign_hourly_seed;

CREATE TABLE IF NOT EXISTS tmp_campaign_hourly_clicks_seed AS mv_campaign_hourly_clicks
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, hour);

CREATE TABLE IF NOT EXISTS tmp_campaign_hourly_conv_seed AS mv_campaign_hourly_conversions
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, hour);

INSERT INTO tmp_campaign_hourly_seed (campaign_id, hour, impression_count)
SELECT
    toUUID(concat('00000000-0000-4000-8000-', lpad(toString(number % 50), 12, '0'))) AS campaign_id,
    toStartOfHour(now() - toIntervalHour(number % 720)) AS hour,
    10 + (number % 20) AS impression_count
FROM numbers(200000);

INSERT INTO tmp_campaign_hourly_clicks_seed (campaign_id, hour, click_count)
SELECT
    toUUID(concat('00000000-0000-4000-8000-', lpad(toString(number % 50), 12, '0'))) AS campaign_id,
    toStartOfHour(now() - toIntervalHour(number % 720)) AS hour,
    number % 5 AS click_count
FROM numbers(200000);

INSERT INTO tmp_campaign_hourly_conv_seed (campaign_id, hour, conversion_count)
SELECT
    toUUID(concat('00000000-0000-4000-8000-', lpad(toString(number % 50), 12, '0'))) AS campaign_id,
    toStartOfHour(now() - toIntervalHour(number % 720)) AS hour,
    number % 2 AS conversion_count
FROM numbers(100000);

-- Hourly stats query used by management service (union of three MVs).
EXPLAIN indexes = 1
SELECT
    hour,
    sum(impressions) AS impressions,
    sum(clicks) AS clicks,
    sum(conversions) AS conversions
FROM (
    SELECT hour, impression_count AS impressions, toUInt64(0) AS clicks, toUInt64(0) AS conversions
    FROM tmp_campaign_hourly_seed
    WHERE campaign_id = toUUID('00000000-0000-4000-8000-000000000042')
      AND hour >= now() - INTERVAL 7 DAY
      AND hour < now()
    UNION ALL
    SELECT hour, toUInt64(0), click_count, toUInt64(0)
    FROM tmp_campaign_hourly_clicks_seed
    WHERE campaign_id = toUUID('00000000-0000-4000-8000-000000000042')
      AND hour >= now() - INTERVAL 7 DAY
      AND hour < now()
    UNION ALL
    SELECT hour, toUInt64(0), toUInt64(0), conversion_count
    FROM tmp_campaign_hourly_conv_seed
    WHERE campaign_id = toUUID('00000000-0000-4000-8000-000000000042')
      AND hour >= now() - INTERVAL 7 DAY
      AND hour < now()
)
GROUP BY hour
ORDER BY hour;

-- Ingestion lag probe for stale flag.
EXPLAIN indexes = 1
SELECT max(latest) FROM (
    SELECT max(created_at) AS latest FROM impressions
    UNION ALL
    SELECT max(created_at) FROM clicks
    UNION ALL
    SELECT max(created_at) FROM conversions
);

DROP TABLE IF EXISTS tmp_campaign_hourly_conv_seed;
DROP TABLE IF EXISTS tmp_campaign_hourly_clicks_seed;
DROP TABLE IF EXISTS tmp_campaign_hourly_seed;
