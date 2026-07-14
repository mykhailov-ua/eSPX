-- Staging manual gate: compare 24h ML shadow scores against ivt-detector labels.
-- Run in staging ClickHouse after ML_ANALYTICS_ENABLED=true for >= 24h.
--
-- Labels:
--   positive = IPs enqueued by ivt-detector (blacklist:fraud path) in the window
--   negative = clean control cohort IPs with no IVT signal
--
-- Example:
--   clickhouse-client --query "$(cat scripts/ml/shadow_precision_report.sql)"

WITH
    shadow AS (
        SELECT
            ip_address,
            max(score) AS ml_score
        FROM ml_shadow_scores
        WHERE created_at >= now() - INTERVAL 24 HOUR
        GROUP BY ip_address
    ),
    positives AS (
        SELECT DISTINCT ip_address
        FROM fraud_events
        WHERE created_at >= now() - INTERVAL 24 HOUR
          AND fraud_reason != ''
    ),
    negatives AS (
        SELECT DISTINCT ip_address
        FROM impressions
        WHERE created_at >= now() - INTERVAL 24 HOUR
          AND ip_address NOT IN (SELECT ip_address FROM positives)
        LIMIT 10000
    ),
    labeled AS (
        SELECT s.ip_address, s.ml_score, 1 AS label
        FROM shadow s
        INNER JOIN positives p USING (ip_address)
        UNION ALL
        SELECT s.ip_address, s.ml_score, 0 AS label
        FROM shadow s
        INNER JOIN negatives n USING (ip_address)
    )
SELECT
    countIf(label = 1 AND ml_score >= 0.6) AS tp,
    countIf(label = 0 AND ml_score >= 0.6) AS fp,
    countIf(label = 1 AND ml_score < 0.6) AS fn,
    countIf(label = 0 AND ml_score < 0.6) AS tn,
    if(tp + fp = 0, 0, tp / (tp + fp)) AS precision,
    if(tp + fn = 0, 0, tp / (tp + fn)) AS recall
FROM labeled;
