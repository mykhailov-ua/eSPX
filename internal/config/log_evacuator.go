package config

import (
	"errors"
	"os"
	"strings"
)

// LogEvacuator holds environment-backed settings for the audit log evacuator binary.
type LogEvacuator struct {
	LogDir             string
	CheckpointPath     string
	S3Region           string
	S3Bucket           string
	S3Prefix           string
	S3Endpoint         string
	S3ForcePathStyle   bool
	MultipartThreshold int64
	ScanIntervalMs     int
}

// LoadLogEvacuator reads evacuator-specific environment variables without requiring the full service config.
func LoadLogEvacuator() (LogEvacuator, error) {
	cfg := LogEvacuator{
		LogDir:             envOrDefault("LOG_EVACUATOR_LOG_DIR", os.Getenv("LOG_DIR")),
		CheckpointPath:     envOrDefault("LOG_EVACUATOR_CHECKPOINT_PATH", "/var/lib/espx/log-evacuator.checkpoint"),
		S3Region:           envOrDefault("LOG_EVACUATOR_S3_REGION", os.Getenv("AWS_REGION")),
		S3Bucket:           os.Getenv("LOG_EVACUATOR_S3_BUCKET"),
		S3Prefix:           strings.Trim(os.Getenv("LOG_EVACUATOR_S3_PREFIX"), "/"),
		S3Endpoint:         os.Getenv("LOG_EVACUATOR_S3_ENDPOINT"),
		S3ForcePathStyle:   getEnvBool("LOG_EVACUATOR_S3_FORCE_PATH_STYLE", false),
		MultipartThreshold: getEnvInt64("LOG_EVACUATOR_MULTIPART_THRESHOLD_BYTES", 8*1024*1024),
		ScanIntervalMs:     getEnvInt("LOG_EVACUATOR_SCAN_INTERVAL_MS", 5000),
	}

	if cfg.LogDir == "" {
		cfg.LogDir = "/var/log/espx"
	}
	if cfg.S3Region == "" {
		return LogEvacuator{}, errors.New("LOG_EVACUATOR_S3_REGION or AWS_REGION is required")
	}
	if cfg.S3Bucket == "" {
		return LogEvacuator{}, errors.New("LOG_EVACUATOR_S3_BUCKET is required")
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
