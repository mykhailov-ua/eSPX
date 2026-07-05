package config

import (
	"errors"
	"os"
	"strings"
)

// LogCompactor holds environment-backed settings for the warm-tier compactor binary.
type LogCompactor struct {
	Backend                  string
	SourceDir                string
	WarmDir                  string
	CheckpointPath           string
	HotMinAgeHours           int
	SampleRate               int
	DeleteSourceAfterCompact bool
	WorkIntervalHours        int
	MetricsPort              string
	ColdEnabled              bool
	ColdCheckpointPath       string
	ColdWarmMinAgeDays       int
	ColdWorkIntervalHours    int
	DeleteWarmAfterCold      bool
	CHDSN                    string
	LeaderElection           bool
	LeaderLockPath           string
	S3Region                 string
	S3Bucket                 string
	S3HotPrefix              string
	S3WarmPrefix             string
	S3ScratchDir             string
	S3Endpoint               string
	S3ForcePathStyle         bool
}

// LoadLogCompactor reads compactor-specific environment variables.
func LoadLogCompactor() (LogCompactor, error) {
	cfg := LogCompactor{
		Backend:                  strings.ToLower(envOrDefault("LOG_COMPACTOR_BACKEND", "local")),
		SourceDir:                envOrDefault("LOG_COMPACTOR_SOURCE_DIR", os.Getenv("LOG_DIR")),
		WarmDir:                  envOrDefault("LOG_COMPACTOR_WARM_DIR", "/var/lib/espx/log-store/warm"),
		CheckpointPath:           envOrDefault("LOG_COMPACTOR_CHECKPOINT_PATH", "/var/lib/espx/log-compactor.checkpoint.jsonl"),
		HotMinAgeHours:           getEnvInt("LOG_COMPACTOR_HOT_MIN_AGE_H", 0),
		SampleRate:               getEnvInt("LOG_COMPACTOR_WARM_SAMPLE_RATE", 1000),
		DeleteSourceAfterCompact: getEnvBool("LOG_COMPACTOR_DELETE_SOURCE", false),
		WorkIntervalHours:        getEnvInt("LOG_COMPACTOR_WORK_INTERVAL_H", 1),
		MetricsPort:              envOrDefault("LOG_COMPACTOR_METRICS_PORT", "9190"),
		ColdCheckpointPath:       envOrDefault("LOG_COMPACTOR_COLD_CHECKPOINT_PATH", "/var/lib/espx/log-compactor-cold.checkpoint.jsonl"),
		ColdWarmMinAgeDays:       getEnvInt("LOG_COMPACTOR_COLD_WARM_MIN_AGE_D", 7),
		ColdWorkIntervalHours:    getEnvInt("LOG_COMPACTOR_COLD_WORK_INTERVAL_H", 24),
		DeleteWarmAfterCold:      getEnvBool("LOG_COMPACTOR_DELETE_WARM_AFTER_COLD", false),
		CHDSN:                    os.Getenv("CH_DSN"),
		LeaderLockPath:           envOrDefault("LOG_COMPACTOR_LEADER_LOCK_PATH", "/var/lib/espx/log-compactor.leader.lock"),
		S3Region:                 envOrDefault("LOG_COMPACTOR_S3_REGION", os.Getenv("AWS_REGION")),
		S3Bucket:                 os.Getenv("LOG_COMPACTOR_S3_BUCKET"),
		S3HotPrefix:              strings.Trim(os.Getenv("LOG_COMPACTOR_S3_HOT_PREFIX"), "/"),
		S3WarmPrefix:             strings.Trim(os.Getenv("LOG_COMPACTOR_S3_WARM_PREFIX"), "/"),
		S3ScratchDir:             envOrDefault("LOG_COMPACTOR_S3_SCRATCH_DIR", "/var/lib/espx/log-compactor/scratch"),
		S3Endpoint:               os.Getenv("LOG_COMPACTOR_S3_ENDPOINT"),
		S3ForcePathStyle:         getEnvBool("LOG_COMPACTOR_S3_FORCE_PATH_STYLE", false),
	}
	cfg.ColdEnabled = getEnvBool("LOG_COMPACTOR_COLD_ENABLED", cfg.CHDSN != "")
	cfg.LeaderElection = getEnvBool("LOG_COMPACTOR_LEADER_ELECTION", false)

	if cfg.SourceDir == "" {
		cfg.SourceDir = "/var/log/espx"
	}
	if cfg.SampleRate <= 0 {
		return LogCompactor{}, errors.New("LOG_COMPACTOR_WARM_SAMPLE_RATE must be positive")
	}
	if cfg.Backend != "local" && cfg.Backend != "s3" {
		return LogCompactor{}, errors.New("LOG_COMPACTOR_BACKEND must be local or s3")
	}
	if cfg.ColdEnabled && cfg.CHDSN == "" {
		return LogCompactor{}, errors.New("LOG_COMPACTOR_COLD_ENABLED requires CH_DSN")
	}
	if cfg.Backend == "s3" && (cfg.S3Bucket == "" || cfg.S3Region == "") {
		return LogCompactor{}, errors.New("LOG_COMPACTOR_S3_BUCKET and LOG_COMPACTOR_S3_REGION are required for s3 backend")
	}

	return cfg, nil
}
