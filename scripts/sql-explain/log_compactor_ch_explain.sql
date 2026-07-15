-- ClickHouse query plans for audit_log_rollups (cold tier from log-compactor).
-- Run: clickhouse-client --multiquery < scripts/sql-explain/log_compactor_ch_explain.sql
-- Requires ad_event_processor.audit_log_rollups (deploy/clickhouse/init.sql).

USE ad_event_processor;

CREATE TABLE IF NOT EXISTS tmp_audit_log_rollups_seed AS audit_log_rollups
ENGINE = MergeTree
PARTITION BY toYYYYMM(rollup_hour)
ORDER BY (campaign_id, rollup_hour, event_type, source_segment, warm_dest_sha256);

TRUNCATE TABLE tmp_audit_log_rollups_seed;

INSERT INTO tmp_audit_log_rollups_seed (
    rollup_hour, campaign_id, event_type,
    event_count, fraud_event_count, billable_event_count,
    sample_click_ids, source_segment, warm_dest_sha256
)
SELECT
    toStartOfHour(now() - toIntervalHour(number % 168)) AS rollup_hour,
    toUUID(concat('00000000-0000-4000-8000-', lpad(toString(number % 200), 12, '0'))) AS campaign_id,
    ['impression', 'click', 'conversion', 'fraud_mark'][1 + (number % 4)] AS event_type,
    50 + (number % 100) AS event_count,
    number % 7 AS fraud_event_count,
    40 + (number % 80) AS billable_event_count,
    [concat('click-', toString(number % 1000))] AS sample_click_ids,
    concat('segment_', toString(number % 500), '.compact.zst') AS source_segment,
    lower(hex(SHA256(toString(number)))) AS warm_dest_sha256
FROM numbers(100000);

-- Campaign hourly volume (typical recon / ops dashboard).
EXPLAIN indexes = 1
SELECT
    rollup_hour,
    event_type,
    sum(event_count) AS events,
    sum(fraud_event_count) AS fraud_events
FROM tmp_audit_log_rollups_seed
WHERE campaign_id = toUUID('00000000-0000-4000-8000-000000000042')
  AND rollup_hour >= now() - INTERVAL 7 DAY
GROUP BY rollup_hour, event_type
ORDER BY rollup_hour DESC;

-- Fraud rate by campaign over rolling window.
EXPLAIN indexes = 1
SELECT
    campaign_id,
    sum(event_count) AS total_events,
    sum(fraud_event_count) AS fraud_events,
    if(sum(event_count) = 0, 0, sum(fraud_event_count) / sum(event_count)) AS fraud_rate
FROM tmp_audit_log_rollups_seed
WHERE rollup_hour >= now() - INTERVAL 30 DAY
GROUP BY campaign_id
HAVING total_events > 1000
ORDER BY fraud_rate DESC
LIMIT 50;

-- Billable throughput top campaigns (uses billable_event_count sum).
EXPLAIN indexes = 1
SELECT
    campaign_id,
    sum(billable_event_count) AS billable_events
FROM tmp_audit_log_rollups_seed
WHERE rollup_hour >= now() - INTERVAL 24 HOUR
GROUP BY campaign_id
ORDER BY billable_events DESC
LIMIT 20;

-- Partition pruning: bounded range within seeded data window.
EXPLAIN indexes = 1
SELECT
    toStartOfMonth(rollup_hour) AS month,
    sum(event_count) AS events
FROM tmp_audit_log_rollups_seed
WHERE rollup_hour >= now() - INTERVAL 90 DAY
  AND rollup_hour < now()
GROUP BY month
ORDER BY month;

DROP TABLE tmp_audit_log_rollups_seed;
