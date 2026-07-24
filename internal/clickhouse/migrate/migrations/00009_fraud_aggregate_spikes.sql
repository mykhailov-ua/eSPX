-- M11: subnet/reason fraud spike aggregates from adaptive FraudStreamWriter flush worker.
CREATE TABLE IF NOT EXISTS fraud_aggregate_spikes (
    subnet String,
    fraud_reason LowCardinality(String),
    event_count UInt64,
    window_ms UInt32,
    created_at DateTime64(3, 'UTC')
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (subnet, fraud_reason, created_at)
TTL toDateTime(created_at) + INTERVAL 90 DAY;
