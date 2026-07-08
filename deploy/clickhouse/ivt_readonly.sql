-- ClickHouse read-only user for IVT detector analytics queries (M7.15).
CREATE USER IF NOT EXISTS ivt_readonly IDENTIFIED BY 'change-me-in-production';

GRANT SELECT ON espx.clicks TO ivt_readonly;
GRANT SELECT ON espx.impressions TO ivt_readonly;
GRANT SELECT ON espx.conversions TO ivt_readonly;
