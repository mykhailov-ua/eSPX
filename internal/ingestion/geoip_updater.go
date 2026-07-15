package ingestion

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"espx/internal/metrics"
)

// GeoIPUpdaterConfig controls MaxMind database refresh on a shared volume.
type GeoIPUpdaterConfig struct {
	DBPath         string
	StagingPath    string
	EditionID      string
	LicenseKey     string
	UpdateInterval time.Duration
	HTTPClient     *http.Client
}

// GeoIPUpdater downloads GeoLite2 archives and atomically replaces the active mmdb file.
type GeoIPUpdater struct {
	cfg GeoIPUpdaterConfig
}

// NewGeoIPUpdater constructs an updater for the given paths and interval.
func NewGeoIPUpdater(cfg GeoIPUpdaterConfig) *GeoIPUpdater {
	if cfg.DBPath == "" {
		cfg.DBPath = "deploy/geoip/GeoLite2-Country.mmdb"
	}
	if cfg.StagingPath == "" {
		cfg.StagingPath = cfg.DBPath + ".staging"
	}
	if cfg.EditionID == "" {
		cfg.EditionID = "GeoLite2-Country"
	}
	if cfg.UpdateInterval <= 0 {
		cfg.UpdateInterval = 24 * time.Hour
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Minute}
	}
	return &GeoIPUpdater{cfg: cfg}
}

// Start runs periodic update checks until the context is cancelled.
func (u *GeoIPUpdater) Start(ctx context.Context) {
	if u == nil {
		return
	}

	ticker := time.NewTicker(u.cfg.UpdateInterval)
	defer ticker.Stop()

	u.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			u.runOnce(ctx)
		}
	}
}

func (u *GeoIPUpdater) runOnce(ctx context.Context) {
	if u.cfg.LicenseKey == "" {
		slog.Debug("geoip updater skipped: MAXMIND_LICENSE_KEY not configured")
		return
	}

	if err := u.downloadAndInstall(ctx); err != nil {
		metrics.GeoIPUpdateErrorsTotal.Inc()
		slog.Warn("geoip updater cycle failed", "error", err)
		return
	}
	slog.Info("geoip database refreshed", "path", u.cfg.DBPath)
}

func (u *GeoIPUpdater) downloadAndInstall(ctx context.Context) error {
	url := fmt.Sprintf(
		"https://download.maxmind.com/app/geoip_download?edition_id=%s&license_key=%s&suffix=tar.gz",
		u.cfg.EditionID,
		u.cfg.LicenseKey,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := u.cfg.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("download maxmind archive: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("maxmind download status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if err := os.MkdirAll(filepath.Dir(u.cfg.StagingPath), 0o755); err != nil {
		return fmt.Errorf("create staging dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(u.cfg.StagingPath), "geoip-*.tar.gz")
	if err != nil {
		return fmt.Errorf("create temp archive: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
	}()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		return fmt.Errorf("write archive: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	archive, err := os.Open(tmpName)
	if err != nil {
		return err
	}
	defer archive.Close()

	gzr, err := gzip.NewReader(archive)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	var extracted bool
	for {
		hdr, readErr := tr.Next()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read tar: %w", readErr)
		}
		if hdr.Typeflag != tar.TypeReg || !strings.HasSuffix(hdr.Name, ".mmdb") {
			continue
		}

		out, createErr := os.OpenFile(u.cfg.StagingPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if createErr != nil {
			return fmt.Errorf("create staging mmdb: %w", createErr)
		}
		if _, copyErr := io.Copy(out, tr); copyErr != nil {
			_ = out.Close()
			return fmt.Errorf("extract mmdb: %w", copyErr)
		}
		if closeErr := out.Close(); closeErr != nil {
			return closeErr
		}
		extracted = true
		break
	}
	if !extracted {
		return fmt.Errorf("no .mmdb file found in maxmind archive")
	}

	if err := os.Rename(u.cfg.StagingPath, u.cfg.DBPath); err != nil {
		return fmt.Errorf("atomic install: %w", err)
	}
	return nil
}
