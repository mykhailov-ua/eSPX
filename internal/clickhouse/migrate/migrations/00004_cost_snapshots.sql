-- Cost snapshots for placement-level spend/revenue rollup (M16 feed for M17 margin guard).

USE ad_event_processor;

CREATE TABLE IF NOT EXISTS cost_snapshots (
    snapshot_hour DateTime,
    customer_id UUID,
    campaign_id UUID,
    network LowCardinality(String),
    placement_id String,
    line_type LowCardinality(String),
    amount_usd_micro Int64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(snapshot_hour)
ORDER BY (campaign_id, placement_id, network, line_type, snapshot_hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_placement_stats_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (campaign_id, placement_id, hour)
POPULATE
AS SELECT
    campaign_id,
    placement_id,
    toStartOfHour(snapshot_hour) AS hour,
    sumIf(amount_usd_micro, line_type = 'spend') AS spend_micro,
    sumIf(amount_usd_micro, line_type = 'revenue') AS revenue_micro,
    count() AS snapshot_count
FROM cost_snapshots
GROUP BY campaign_id, placement_id, hour;
