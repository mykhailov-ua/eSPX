-- Processor ClickHouse migrations: hourly campaign aggregates for reporting and recon.
-- Applied idempotently on processor startup (compose also mounts deploy/clickhouse/recon_materialized_views.sql).

USE ad_event_processor;

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
