-- M13: ZSTD codec on raw event payload columns; merges pick up codec on OPTIMIZE FINAL.
ALTER TABLE impressions MODIFY COLUMN payload CODEC(ZSTD(3));
ALTER TABLE clicks MODIFY COLUMN payload CODEC(ZSTD(3));
ALTER TABLE conversions MODIFY COLUMN payload CODEC(ZSTD(3));
ALTER TABLE fraud_events MODIFY COLUMN payload CODEC(ZSTD(3));
