-- Staging gate: compare 24h shadow fraud scores against ivt-detector labels.
-- Run after FRAUD_SCORING_ENABLED=true for >= 24h.
-- See docs/ARCHITECTURE.md#fraud-scoring-cold-path and docs/DATABASE.md Part II §5.
--
-- Example:
--   clickhouse-client --query "$(cat scripts/fraud-scoring/shadow_precision_report.sql)"

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
