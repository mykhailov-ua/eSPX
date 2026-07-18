package licensing

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"espx/internal/billing/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type LicenseWatcher struct {
	pool       *pgxpool.Pool
	rdb        redis.UniversalClient
	client     *LicenseClient
	mode       string // file | online
	path       string
	spoolDir   string
	serverURL  string
	licenseKey string
	interval   time.Duration
	timeout    time.Duration
	spool      *LicenseSpool

	mu               sync.RWMutex
	currentClaims    *LicenseClaims
	currentState     LicenseState
	lastVerifiedAt   time.Time
	lastRefreshError error
	pubKey           ed25519.PublicKey
}

func NewLicenseWatcher(pool *pgxpool.Pool, rdb redis.UniversalClient, pubKey ed25519.PublicKey) *LicenseWatcher {
	mode := os.Getenv("ESPX_LICENSE_MODE")
	if mode == "" {
		mode = "file"
	}
	path := os.Getenv("ESPX_LICENSE_PATH")
	if path == "" {
		path = "license.jwt" // Default for dev
	}
	spoolDir := os.Getenv("ESPX_LICENSE_SPOOL_DIR")
	if spoolDir == "" {
		spoolDir = filepath.Join(filepath.Dir(path), ".license-spool")
	}
	serverURL := os.Getenv("ESPX_LICENSE_SERVER")
	if serverURL == "" {
		serverURL = "https://license.espx.io"
	}
	licenseKey := os.Getenv("ESPX_LICENSE_KEY")

	refreshStr := os.Getenv("ESPX_LICENSE_REFRESH_INTERVAL")
	interval := 24 * time.Hour
	if d, err := time.ParseDuration(refreshStr); err == nil {
		interval = d
	}

	timeoutStr := os.Getenv("ESPX_LICENSE_HEARTBEAT_TIMEOUT")
	timeout := 5 * time.Second
	if d, err := time.ParseDuration(timeoutStr); err == nil {
		timeout = d
	}

	client := NewLicenseClient(serverURL, licenseKey, timeout)

	return &LicenseWatcher{
		pool:         pool,
		rdb:          rdb,
		client:       client,
		mode:         mode,
		path:         path,
		spoolDir:     spoolDir,
		serverURL:    serverURL,
		licenseKey:   licenseKey,
		interval:     interval,
		timeout:      timeout,
		pubKey:       pubKey,
		currentState: StateExpired,
	}
}

func (w *LicenseWatcher) openSpool() error {
	if w.spool != nil {
		return nil
	}
	spool, err := OpenLicenseSpool(w.spoolDir)
	if err != nil {
		return fmt.Errorf("open license spool: %w", err)
	}
	w.spool = spool
	return nil
}

func (w *LicenseWatcher) closeSpool() {
	if w.spool != nil {
		_ = w.spool.Close()
		w.spool = nil
	}
}

func (w *LicenseWatcher) GetState() (LicenseState, *LicenseClaims) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.currentState, w.currentClaims
}

func (w *LicenseWatcher) Start(ctx context.Context) error {
	if err := w.openSpool(); err != nil {
		slog.Error("license spool open failed", "error", err)
	}

	// Recover durable WAL token before first verify.
	if w.spool != nil {
		if token, err := w.spool.LatestToken(); err != nil {
			slog.Warn("license spool recovery failed", "error", err)
		} else if token != "" {
			if err := os.WriteFile(w.path, []byte(token), 0o600); err != nil {
				slog.Warn("failed to hydrate license file from spool", "error", err)
			} else {
				slog.Info("recovered license token from mmap spool")
			}
		}
	}

	// 1. Initial verify on startup
	if err := w.verifyAndReload(ctx); err != nil {
		slog.Error("Initial license verification failed", "error", err)
	}

	// 2. Ticker loop
	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		defer w.closeSpool()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.verifyAndReload(ctx); err != nil {
					slog.Error("Scheduled license refresh failed", "error", err)
				}
			}
		}
	}()

	return nil
}

func (w *LicenseWatcher) verifyAndReload(ctx context.Context) error {
	var tokenStr string
	var err error

	// If mode is online, try performing heartbeat first
	if w.mode == "online" && w.licenseKey != "" {
		tokenStr, err = w.performOnlineHeartbeat(ctx)
		if err != nil {
			slog.Warn("Online license heartbeat failed, falling back to cached file", "error", err)
			w.mu.Lock()
			w.lastRefreshError = err
			w.mu.Unlock()
		}
	}

	// Fallback to local file if online failed or is in file mode
	if tokenStr == "" {
		tokenStr, err = w.readLocalFile()
		if err != nil {
			w.mu.Lock()
			w.lastRefreshError = err
			w.currentState = StateExpired
			w.mu.Unlock()
			return fmt.Errorf("failed to read local license file: %w", err)
		}
	}

	// Verify JWT
	claims, err := VerifyJWT(tokenStr, w.pubKey)
	if err != nil {
		w.mu.Lock()
		w.lastRefreshError = err
		w.currentState = StateExpired
		w.mu.Unlock()
		return fmt.Errorf("license signature verification failed: %w", err)
	}

	// Determine state
	state := DetermineState(claims, time.Now(), false)

	w.mu.Lock()
	w.currentClaims = claims
	w.currentState = state
	w.lastVerifiedAt = time.Now()
	w.lastRefreshError = nil
	w.mu.Unlock()

	// If state is EXPIRED, we don't proceed with DB/Redis updates of active state but record the state
	err = w.updateDatabaseAndRedis(ctx, tokenStr, claims, state)
	if err != nil {
		slog.Error("Failed to update license status in DB/Redis", "error", err)
		return err
	}

	return nil
}

func (w *LicenseWatcher) performOnlineHeartbeat(ctx context.Context) (string, error) {
	// First read cached token to extract deployment ID
	cachedToken, err := w.readLocalFile()
	var deploymentID string
	var fingerprint string
	var uptime int64 = 300 // simulated or tracked uptime

	if err == nil {
		claims, err := DecodeUnverified(cachedToken)
		if err == nil {
			deploymentID = claims.DeploymentID
			fingerprint = claims.Bind.Fingerprint
		}
	}

	if deploymentID == "" {
		deploymentID = uuid.NewString()
	}

	var token string
	if cachedToken == "" {
		// Try activation
		token, err = w.client.Activate(ctx, deploymentID, fingerprint)
		if err != nil {
			return "", err
		}
	} else {
		// Try heartbeat
		var notModified bool
		token, notModified, err = w.client.Heartbeat(ctx, deploymentID, fingerprint, uptime)
		if err != nil {
			return "", err
		}
		if notModified {
			return cachedToken, nil
		}
	}

	// Save received token to durable spool and file cache.
	if err := w.persistLicenseToken(token); err != nil {
		slog.Error("Failed to cache license token", "error", err)
	}

	return token, nil
}

func (w *LicenseWatcher) persistLicenseToken(token string) error {
	if err := w.openSpool(); err != nil {
		return err
	}
	if w.spool != nil {
		if err := w.spool.AppendDurably(token); err != nil {
			return fmt.Errorf("spool append: %w", err)
		}
	}
	if err := os.WriteFile(w.path, []byte(token), 0o600); err != nil {
		return fmt.Errorf("write license file: %w", err)
	}
	return nil
}

func (w *LicenseWatcher) readLocalFile() (string, error) {
	if w.spool == nil {
		if err := w.openSpool(); err == nil && w.spool != nil {
			if token, spoolErr := w.spool.LatestToken(); spoolErr == nil && token != "" {
				return token, nil
			}
		}
	} else if token, err := w.spool.LatestToken(); err == nil && token != "" {
		return token, nil
	}
	data, err := os.ReadFile(w.path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (w *LicenseWatcher) updateDatabaseAndRedis(ctx context.Context, token string, claims *LicenseClaims, state LicenseState) error {
	// 1. Update database: billing.license_status
	depID, err := uuid.Parse(claims.DeploymentID)
	if err != nil {
		return fmt.Errorf("invalid deployment id in claims: %w", err)
	}
	licID, err := uuid.Parse(claims.Subject)
	if err != nil {
		licID = uuid.Nil
	}

	entitlements := Entitlements{
		Limits:   claims.Limits,
		Features: claims.Features,
	}
	entitlementsJSON, err := json.Marshal(entitlements)
	if err != nil {
		return err
	}

	queries := db.New(w.pool)
	var errStr pgtype.Text
	if w.lastRefreshError != nil {
		errStr = pgtype.Text{String: w.lastRefreshError.Error(), Valid: true}
	}

	_, err = queries.UpsertLicenseStatus(ctx, db.UpsertLicenseStatusParams{
		DeploymentID:     pgtype.UUID{Bytes: depID, Valid: true},
		LicenseID:        pgtype.UUID{Bytes: licID, Valid: true},
		PlanCode:         claims.Plan,
		ValidUntil:       pgtype.Timestamptz{Time: claims.ValidUntil, Valid: true},
		State:            string(state),
		EntitlementsJson: entitlementsJSON,
		LastVerifiedAt:   pgtype.Timestamptz{Time: w.lastVerifiedAt, Valid: true},
		LastRefreshError: errStr,
	})
	if err != nil {
		return fmt.Errorf("database update failed: %w", err)
	}

	// 2. Update Redis snapshot: entitlement:deployment
	redisKey := "entitlement:deployment"
	fields := map[string]any{
		"state":                string(state),
		"plan":                 claims.Plan,
		"valid_until":          claims.ValidUntil.Format(time.RFC3339),
		"max_rps":              claims.Limits.MaxRPS,
		"max_requests_per_day": claims.Limits.MaxRequestsPerDay,
		"rtb_live":             boolToInt(claims.Features.RtbLive),
		"ml_fraud_boost":       boolToInt(claims.Features.MlFraudBoost),
		"multi_region":         boolToInt(claims.Features.MultiRegion),
		"slot_migration":       boolToInt(claims.Features.SlotMigration),
	}

	if err := w.rdb.HMSet(ctx, redisKey, fields).Err(); err != nil {
		return fmt.Errorf("redis HMSet failed: %w", err)
	}

	// Publish registry refresh notification
	_ = w.rdb.Publish(ctx, "campaigns:update", "license_update")

	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
