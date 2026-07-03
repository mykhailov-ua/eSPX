package config

import (
	"errors"
	"os"
)

// IVTDetector holds environment-backed settings for the IVT anomaly detector binary.
type IVTDetector struct {
	ScanIntervalMs       int
	OutboxPendingLimit   int64
	ManagementURL        string
	ManagementTimeoutMs  int
	WindowSec            int
	MinClicks            uint64
	MinImpressions       uint64
	ClickToImpRatio      float64
	MinIPsPerUA          uint64
	AdminAPIKey          Secret
	DBDSN                Secret
	DBMaxConns           int
	DBMinConns           int
	CHDSN                Secret
}

// LoadIVTDetector reads ivt-detector-specific environment variables.
func LoadIVTDetector() (IVTDetector, error) {
	cfg := IVTDetector{
		ScanIntervalMs:      getEnvInt("IVT_DETECTOR_SCAN_INTERVAL_MS", 300000),
		OutboxPendingLimit:  getEnvInt64("IVT_DETECTOR_OUTBOX_PENDING_LIMIT", 500),
		ManagementURL:       os.Getenv("MANAGEMENT_URL"),
		ManagementTimeoutMs: getEnvInt("IVT_DETECTOR_MANAGEMENT_TIMEOUT_MS", 10000),
		WindowSec:           getEnvInt("IVT_DETECTOR_WINDOW_SEC", 3600),
		MinClicks:           uint64(getEnvInt64("IVT_DETECTOR_MIN_CLICKS", 10)),
		MinImpressions:      uint64(getEnvInt64("IVT_DETECTOR_MIN_IMPRESSIONS", 1)),
		ClickToImpRatio:     getEnvFloat("IVT_DETECTOR_CLICK_TO_IMP_RATIO", 5.0),
		MinIPsPerUA:         uint64(getEnvInt64("IVT_DETECTOR_MIN_IPS_PER_UA", 8)),
		AdminAPIKey:         Secret(os.Getenv("ADMIN_API_KEY")),
		DBDSN:               Secret(os.Getenv("DB_DSN")),
		DBMaxConns:          getEnvInt("DB_TRACKER_MAX_CONNS", 10),
		DBMinConns:          getEnvInt("DB_MIN_CONNS", 2),
		CHDSN:               Secret(os.Getenv("CH_DSN")),
	}

	if cfg.ManagementURL == "" {
		port := os.Getenv("MANAGEMENT_PORT")
		if port == "" {
			port = "8188"
		}
		cfg.ManagementURL = "http://127.0.0.1:" + port
	}
	if string(cfg.DBDSN) == "" {
		return IVTDetector{}, errors.New("DB_DSN is required")
	}
	if string(cfg.CHDSN) == "" {
		return IVTDetector{}, errors.New("CH_DSN is required")
	}
	if string(cfg.AdminAPIKey) == "" {
		return IVTDetector{}, errors.New("ADMIN_API_KEY is required")
	}

	return cfg, nil
}
