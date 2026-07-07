-- RTB deal auction outcomes for bid-floor optimizer (M3.7).
-- outcome: 1 = win (bid cleared), 0 = no_bid / loss.

CREATE TABLE IF NOT EXISTS rtb_deal_outcomes (
    deal_id LowCardinality(String),
    outcome UInt8,
    floor_micro Int64,
    created_at DateTime64(3, 'UTC')
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (deal_id, created_at)
TTL toDateTime(created_at) + INTERVAL 90 DAY;
