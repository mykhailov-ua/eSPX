package costsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	db "espx/internal/ingestion/sqlc"
	"espx/internal/metrics"
	"espx/internal/postback"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const costSyncAdvisoryLockKey = int64(0x657370785f636f73) // espx_cos

// Worker ingests network spend and RSOC revenue on an hourly cron schedule.
type Worker struct {
	pool          *pgxpool.Pool
	converter     *CurrencyConverter
	providers     map[string]Provider
	oauth         map[string]OAuthRefresher
	chInserter    snapshotInserter
	encryptionKey []byte
	httpClient    *http.Client

	onSyncComplete func(network string, duration time.Duration)
}

type snapshotInserter interface {
	InsertSnapshots(ctx context.Context, lines []CostLine, usdMicro []int64) error
}

// WorkerOption configures optional worker dependencies.
type WorkerOption func(*Worker)

// WithClickHouse attaches a ClickHouse snapshot inserter.
func WithClickHouse(inserter *ClickHouseInserter) WorkerOption {
	return func(w *Worker) {
		if inserter != nil {
			w.chInserter = inserter
		}
	}
}

// WithMemorySnapshots uses an in-memory CH sink (tests).
func WithMemorySnapshots(m *MemorySnapshotInserter) WorkerOption {
	return func(w *Worker) {
		w.chInserter = m
	}
}

// WithOAuthRefresher registers a network-specific token refresher.
func WithOAuthRefresher(network string, refresher OAuthRefresher) WorkerOption {
	return func(w *Worker) {
		w.oauth[network] = refresher
	}
}

// WithProvider overrides or adds a fetch provider (tests).
func WithProvider(p Provider) WorkerOption {
	return func(w *Worker) {
		w.providers[p.Network()] = p
	}
}

// WithSyncCompleteHook is a test hook invoked after each network sync.
func WithSyncCompleteHook(fn func(network string, duration time.Duration)) WorkerOption {
	return func(w *Worker) {
		w.onSyncComplete = fn
	}
}

// NewWorker constructs the cost sync worker with default network providers.
func NewWorker(pool *pgxpool.Pool, encryptionKey []byte, opts ...WorkerOption) *Worker {
	key := normalizeKey(encryptionKey)
	client := &http.Client{Timeout: 90 * time.Second}

	w := &Worker{
		pool:          pool,
		converter:     NewCurrencyConverter(pool, client),
		encryptionKey: key,
		httpClient:    client,
		providers: map[string]Provider{
			"facebook":     &FacebookProvider{Client: client},
			"taboola":      &TaboolaProvider{Client: client},
			"outbrain":     &OutbrainProvider{Client: client},
			"google":       &GoogleAdsProvider{Client: client},
			"tonic_rsoc":   &TonicRSOCProvider{Client: client},
			"system1_rsoc": &System1RSOCProvider{Client: client},
		},
		oauth: make(map[string]OAuthRefresher),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func normalizeKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte("postback-encryption-secret-key32")
	}
	if len(key) < 32 {
		padded := make([]byte, 32)
		copy(padded, key)
		return padded
	}
	if len(key) > 32 {
		return key[:32]
	}
	return key
}

// Start runs the hourly cron loop until ctx is cancelled.
func (w *Worker) Start(ctx context.Context) {
	slog.Info("cost-sync worker starting", "interval", "1h")
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	w.runHourly(ctx, "cron")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runHourly(ctx, "cron")
		}
	}
}

// RunManual triggers sync for optional customer/network/date range (admin API).
func (w *Worker) RunManual(ctx context.Context, customerID *uuid.UUID, network string, from, to time.Time) error {
	if to.Before(from) {
		return fmt.Errorf("invalid date range")
	}
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		if err := w.syncDay(ctx, customerID, network, d, "manual"); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) runHourly(ctx context.Context, trigger string) {
	opCtx, cancel := context.WithTimeout(ctx, 110*time.Second)
	defer cancel()

	acquired, err := w.tryAdvisoryLock(opCtx)
	if err != nil {
		slog.Error("cost-sync advisory lock failed", "error", err)
		return
	}
	if !acquired {
		slog.Debug("cost-sync skipped: another leader holds lock")
		return
	}
	defer w.releaseAdvisoryLock(context.Background())

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	if err := w.syncDay(opCtx, nil, "", yesterday, trigger); err != nil {
		slog.Error("cost-sync hourly run failed", "error", err)
		metrics.CostSyncRunsTotal.WithLabelValues("failed").Inc()
	}
}

func (w *Worker) syncDay(ctx context.Context, filterCustomer *uuid.UUID, filterNetwork string, date time.Time, trigger string) error {
	q := db.New(w.pool)
	creds, err := q.ListCostSyncCredentials(ctx)
	if err != nil {
		return err
	}

	for _, credRow := range creds {
		if filterCustomer != nil && credRow.CustomerID.Bytes != *filterCustomer {
			continue
		}
		if filterNetwork != "" && credRow.Network != filterNetwork {
			continue
		}
		if err := w.syncCredential(ctx, credRow, date, trigger); err != nil {
			slog.Warn("cost-sync network failed", "network", credRow.Network, "customer_id", credRow.CustomerID.Bytes, "error", err)
			metrics.CostSyncRunsTotal.WithLabelValues("failed").Inc()
		}
	}
	return nil
}

func (w *Worker) syncCredential(ctx context.Context, credRow db.CostSyncCredential, date time.Time, trigger string) error {
	start := time.Now()
	network := credRow.Network
	provider, ok := w.providers[network]
	if !ok {
		return fmt.Errorf("unsupported network: %s", network)
	}

	run, err := db.New(w.pool).InsertCostSyncRun(ctx, db.InsertCostSyncRunParams{
		CustomerID:    credRow.CustomerID,
		Network:       network,
		CostDate:      pgtype.Date{Time: date, Valid: true},
		Status:        "RUNNING",
		TriggerSource: trigger,
	})
	if err != nil {
		return err
	}

	cred, err := w.decryptCredential(credRow)
	if err != nil {
		w.completeRun(ctx, run.ID, "FAILED", 0, 0, err.Error())
		return err
	}

	if err := w.maybeRefreshToken(ctx, network, credRow, &cred); err != nil {
		w.completeRun(ctx, run.ID, "FAILED", 0, 0, err.Error())
		return err
	}

	lines, err := provider.Fetch(ctx, cred, date)
	if err != nil {
		w.completeRun(ctx, run.ID, "FAILED", 0, 0, err.Error())
		metrics.CostSyncRunsTotal.WithLabelValues("failed").Inc()
		return err
	}

	imported, totalUSD, err := w.persistLines(ctx, lines, date)
	if err != nil {
		w.completeRun(ctx, run.ID, "FAILED", imported, totalUSD, err.Error())
		metrics.CostSyncRunsTotal.WithLabelValues("failed").Inc()
		return err
	}

	if err := w.reconcileCampaigns(ctx, lines, date); err != nil {
		slog.Warn("cost-sync reconciliation partial failure", "error", err)
	}

	w.completeRun(ctx, run.ID, "COMPLETED", imported, totalUSD, "")
	metrics.CostSyncRunsTotal.WithLabelValues("success").Inc()
	metrics.CostSyncRowsImported.Add(float64(imported))
	metrics.CostSyncDurationSeconds.WithLabelValues(network).Observe(time.Since(start).Seconds())

	if w.onSyncComplete != nil {
		w.onSyncComplete(network, time.Since(start))
	}
	return nil
}

func (w *Worker) persistLines(ctx context.Context, lines []CostLine, date time.Time) (int, int64, error) {
	if len(lines) == 0 {
		return 0, 0, nil
	}

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)

	q := db.New(tx)
	usdAmounts := make([]int64, len(lines))

	var imported int
	var totalUSD int64
	for i, line := range lines {
		usdMicro, err := w.converter.ToUSDMicro(ctx, line.AmountMicro, line.Currency, date)
		if err != nil {
			return imported, totalUSD, err
		}
		usdAmounts[i] = usdMicro
		totalUSD += usdMicro

		ingestKey := IngestKey(line.CustomerID, line.CampaignID, line.Date, line.Network, line.PlacementID, line.LineType)
		rows, err := q.InsertCampaignCost(ctx, db.InsertCampaignCostParams{
			CustomerID:     pgtype.UUID{Bytes: line.CustomerID, Valid: true},
			CampaignID:     pgtype.UUID{Bytes: line.CampaignID, Valid: true},
			CostDate:       pgtype.Date{Time: line.Date, Valid: true},
			Network:        line.Network,
			PlacementID:    line.PlacementID,
			AdsetID:        line.AdsetID,
			AdID:           line.AdID,
			LineType:       string(line.LineType),
			AmountMicro:    line.AmountMicro,
			Currency:       line.Currency,
			AmountUsdMicro: usdMicro,
			IngestKey:      ingestKey,
		})
		if err != nil {
			return imported, totalUSD, err
		}
		if rows > 0 {
			imported++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return imported, totalUSD, err
	}

	if w.chInserter != nil {
		if err := w.chInserter.InsertSnapshots(ctx, lines, usdAmounts); err != nil {
			slog.Warn("cost-sync clickhouse insert failed", "error", err)
			metrics.CostSyncCHErrors.Inc()
		}
	}

	return imported, totalUSD, nil
}

func (w *Worker) reconcileCampaigns(ctx context.Context, lines []CostLine, date time.Time) error {
	seen := make(map[uuid.UUID]struct{})
	for _, line := range lines {
		if line.LineType != LineTypeSpend {
			continue
		}
		seen[line.CampaignID] = struct{}{}
	}

	q := db.New(w.pool)
	for campID := range seen {
		apiSpend, err := q.SumCampaignCostsUSDForDate(ctx, db.SumCampaignCostsUSDForDateParams{
			CampaignID: pgtype.UUID{Bytes: campID, Valid: true},
			CostDate:   pgtype.Date{Time: date, Valid: true},
		})
		if err != nil {
			return err
		}
		trackerSpend, err := q.SumTrackerEstimatedSpendForDate(ctx, db.SumTrackerEstimatedSpendForDateParams{
			CampaignID: pgtype.UUID{Bytes: campID, Valid: true},
			Column2:    pgtype.Date{Time: date, Valid: true},
		})
		if err != nil {
			return err
		}

		delta := apiSpend - trackerSpend
		if delta == 0 {
			continue
		}

		var customerID pgtype.UUID
		err = w.pool.QueryRow(ctx, `SELECT customer_id FROM campaigns WHERE id = $1`, campID).Scan(&customerID)
		if err != nil {
			return err
		}

		hash := reconciliationHash(customerID.Bytes, campID, date)
		_, err = w.pool.Exec(ctx, `
			INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, idempotency_hash)
			VALUES ($1, $2, $3, 'RECONCILIATION_ADJUST', $4)
			ON CONFLICT (idempotency_hash) DO NOTHING`,
			customerID, campID, -delta, hash,
		)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		metrics.CostSyncReconciliationDelta.Add(float64(abs64(delta)))
	}
	return nil
}

func reconciliationHash(customerID, campaignID uuid.UUID, date time.Time) string {
	raw := fmt.Sprintf("cost_sync_recon|%s|%s|%s", customerID, campaignID, date.Format("2006-01-02"))
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func (w *Worker) decryptCredential(row db.CostSyncCredential) (Credential, error) {
	cred := Credential{
		CustomerID: row.CustomerID.Bytes,
		Network:    row.Network,
		AccountID:  row.AccountID,
	}
	if len(row.AccessTokenEncrypted) > 0 {
		b, err := postback.DecryptAESGCM(row.AccessTokenEncrypted, w.encryptionKey)
		if err != nil {
			return cred, err
		}
		cred.AccessToken = string(b)
	}
	if len(row.RefreshTokenEncrypted) > 0 {
		b, err := postback.DecryptAESGCM(row.RefreshTokenEncrypted, w.encryptionKey)
		if err != nil {
			return cred, err
		}
		cred.RefreshToken = string(b)
	}
	if len(row.ApiKeyEncrypted) > 0 {
		b, err := postback.DecryptAESGCM(row.ApiKeyEncrypted, w.encryptionKey)
		if err != nil {
			return cred, err
		}
		cred.APIKey = string(b)
	}
	if len(row.ExtraConfig) > 0 {
		_ = json.Unmarshal(row.ExtraConfig, &cred.ExtraConfig)
	}
	if row.TokenExpiresAt.Valid {
		cred.ExpiresAt = row.TokenExpiresAt.Time
	}
	return cred, nil
}

func (w *Worker) maybeRefreshToken(ctx context.Context, network string, row db.CostSyncCredential, cred *Credential) error {
	refresher, ok := w.oauth[network]
	if !ok {
		return nil
	}
	if !cred.ExpiresAt.IsZero() && time.Until(cred.ExpiresAt) > 5*time.Minute {
		return nil
	}

	token, expires, err := refresher.Refresh(ctx, *cred)
	if err != nil {
		return err
	}
	cred.AccessToken = token
	cred.ExpiresAt = expires

	enc, err := postback.EncryptAESGCM([]byte(token), w.encryptionKey)
	if err != nil {
		return err
	}
	_, err = db.New(w.pool).UpsertCostSyncCredential(ctx, db.UpsertCostSyncCredentialParams{
		CustomerID:            row.CustomerID,
		Network:               row.Network,
		AccountID:             row.AccountID,
		AccessTokenEncrypted:  enc,
		RefreshTokenEncrypted: row.RefreshTokenEncrypted,
		ApiKeyEncrypted:       row.ApiKeyEncrypted,
		ExtraConfig:           row.ExtraConfig,
		TokenExpiresAt:        pgtype.Timestamptz{Time: expires, Valid: true},
	})
	return err
}

func (w *Worker) completeRun(ctx context.Context, id int64, status string, rows int, totalUSD int64, errMsg string) {
	var msg pgtype.Text
	if errMsg != "" {
		msg = pgtype.Text{String: errMsg, Valid: true}
	}
	_ = db.New(w.pool).CompleteCostSyncRun(ctx, db.CompleteCostSyncRunParams{
		ID:                  id,
		Status:              status,
		RowsImported:        int32(rows),
		TotalAmountUsdMicro: totalUSD,
		ErrorMessage:        msg,
	})
}

func (w *Worker) tryAdvisoryLock(ctx context.Context) (bool, error) {
	var ok bool
	err := w.pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, costSyncAdvisoryLockKey).Scan(&ok)
	return ok, err
}

func (w *Worker) releaseAdvisoryLock(ctx context.Context) {
	_, _ = w.pool.Exec(ctx, `SELECT pg_advisory_unlock($1)`, costSyncAdvisoryLockKey)
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// EncryptCredentialFields encrypts credential secrets for storage.
func EncryptCredentialFields(key []byte, accessToken, refreshToken, apiKey string) (accessEnc, refreshEnc, apiEnc []byte, err error) {
	key = normalizeKey(key)
	if accessToken != "" {
		accessEnc, err = postback.EncryptAESGCM([]byte(accessToken), key)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if refreshToken != "" {
		refreshEnc, err = postback.EncryptAESGCM([]byte(refreshToken), key)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if apiKey != "" {
		apiEnc, err = postback.EncryptAESGCM([]byte(apiKey), key)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return accessEnc, refreshEnc, apiEnc, nil
}
