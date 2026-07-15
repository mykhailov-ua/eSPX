package ingestion

import (
	"context"
	"log/slog"
	"os"
	"time"

	"espx/internal/metrics"
)

// GeoIPWatcher hot-reloads a MaxMindProvider when the database file changes on disk.
type GeoIPWatcher struct {
	provider *MaxMindProvider
	dbPath   string
	interval time.Duration
}

// NewGeoIPWatcher polls mtime and reloads the provider without restarting the tracker.
func NewGeoIPWatcher(provider *MaxMindProvider, dbPath string, interval time.Duration) *GeoIPWatcher {
	if interval <= 0 {
		interval = time.Minute
	}
	return &GeoIPWatcher{
		provider: provider,
		dbPath:   dbPath,
		interval: interval,
	}
}

// Start watches the database file until the context is cancelled.
func (w *GeoIPWatcher) Start(ctx context.Context) {
	if w == nil || w.provider == nil || w.dbPath == "" {
		return
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	var lastMod time.Time
	if info, err := os.Stat(w.dbPath); err == nil {
		lastMod = info.ModTime()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(w.dbPath)
			if err != nil {
				slog.Debug("geoip watcher stat failed", "path", w.dbPath, "error", err)
				continue
			}
			if !info.ModTime().After(lastMod) {
				continue
			}
			if err := w.provider.Reload(w.dbPath); err != nil {
				metrics.GeoIPReloadErrorsTotal.Inc()
				slog.Warn("geoip hot reload failed", "path", w.dbPath, "error", err)
				continue
			}
			lastMod = info.ModTime()
			slog.Info("geoip database hot-reloaded", "path", w.dbPath, "mtime", lastMod.UTC().Format(time.RFC3339))
		}
	}
}
