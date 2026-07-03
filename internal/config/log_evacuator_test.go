package config

import "testing"

// Guards LoadLogEvacuator requires bucket and region env vars.
func TestLoadLogEvacuator_requiresBucketAndRegion(t *testing.T) {
	t.Setenv("LOG_EVACUATOR_S3_BUCKET", "")
	t.Setenv("LOG_EVACUATOR_S3_REGION", "")
	t.Setenv("AWS_REGION", "")

	if _, err := LoadLogEvacuator(); err == nil {
		t.Fatal("expected error when bucket and region are missing")
	}
}

// Guards LoadLogEvacuator applies defaults for log directory and scan interval.
func TestLoadLogEvacuator_defaults(t *testing.T) {
	t.Setenv("LOG_EVACUATOR_S3_BUCKET", "audit-bucket")
	t.Setenv("LOG_EVACUATOR_S3_REGION", "eu-west-1")
	t.Setenv("LOG_EVACUATOR_LOG_DIR", "")
	t.Setenv("LOG_DIR", "")

	cfg, err := LoadLogEvacuator()
	if err != nil {
		t.Fatalf("load log evacuator config: %v", err)
	}
	if cfg.LogDir != "/var/log/espx" {
		t.Fatalf("log dir default mismatch: %q", cfg.LogDir)
	}
	if cfg.ScanIntervalMs != 5000 {
		t.Fatalf("scan interval default mismatch: %d", cfg.ScanIntervalMs)
	}
	if cfg.S3Bucket != "audit-bucket" {
		t.Fatalf("bucket mismatch: %q", cfg.S3Bucket)
	}
}

// Guards AWS_REGION is used when LOG_EVACUATOR_S3_REGION is unset.
func TestLoadLogEvacuator_awsRegionFallback(t *testing.T) {
	t.Setenv("LOG_EVACUATOR_S3_BUCKET", "audit-bucket")
	t.Setenv("LOG_EVACUATOR_S3_REGION", "")
	t.Setenv("AWS_REGION", "ap-southeast-1")

	cfg, err := LoadLogEvacuator()
	if err != nil {
		t.Fatalf("load log evacuator config: %v", err)
	}
	if cfg.S3Region != "ap-southeast-1" {
		t.Fatalf("region fallback mismatch: %q", cfg.S3Region)
	}
}
