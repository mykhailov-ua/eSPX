-- Cold-tier audit log rollups produced by log-compactor from warm .compact.zst segments.
-- SummingMergeTree merges event_count and fraud_event_count for the same ORDER BY key.
USE ad_event_processor;

CREATE TABLE IF NOT EXISTS audit_log_rollups (
    rollup_hour DateTime('UTC'),
    campaign_id UUID,
    event_type LowCardinality(String),
    event_count UInt64,
    fraud_event_count UInt64,
    billable_event_count UInt64,
    sample_click_ids Array(String),
    source_segment String,
    warm_dest_sha256 String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(rollup_hour)
ORDER BY (campaign_id, rollup_hour, event_type, source_segment, warm_dest_sha256)
TTL rollup_hour + INTERVAL 365 DAY;
