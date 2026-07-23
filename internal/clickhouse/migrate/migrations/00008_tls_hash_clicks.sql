-- M10-C2: passive TLS fingerprint (JA3) on clicks for TCP×UA×JA3 IVT correlation.
ALTER TABLE clicks ADD COLUMN IF NOT EXISTS tls_hash String DEFAULT '' AFTER user_agent;
