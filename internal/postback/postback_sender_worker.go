package postback

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"
)

var (
	ErrNotProTier     = errors.New("Pro or Enterprise plan required")
	ErrDuplicateEvent = errors.New("duplicate postback event ignored")
)

type PostbackWorker struct {
	pool          *pgxpool.Pool
	client        *http.Client
	encryptionKey []byte
	limiters      map[string]*rate.Limiter
	limitersMu    sync.RWMutex

	adapters map[string]PostbackAdapter

	// Test hooks
	onDispatchAttempt func()
}

func NewPostbackWorker(pool *pgxpool.Pool, encryptionKey []byte) *PostbackWorker {
	if len(encryptionKey) == 0 {
		encryptionKey = []byte("postback-encryption-secret-key32")
	} else if len(encryptionKey) < 32 {
		padded := make([]byte, 32)
		copy(padded, encryptionKey)
		encryptionKey = padded
	} else if len(encryptionKey) > 32 {
		encryptionKey = encryptionKey[:32]
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	return &PostbackWorker{
		pool:          pool,
		client:        client,
		encryptionKey: encryptionKey,
		limiters:      make(map[string]*rate.Limiter),
		adapters: map[string]PostbackAdapter{
			"facebook": &FacebookAdapter{},
			"google":   &GoogleAdapter{},
			"tiktok":   &TikTokAdapter{},
			"webhook":  &WebhookAdapter{},
		},
	}
}

func (w *PostbackWorker) Start(ctx context.Context, interval time.Duration) {
	slog.Info("Postback sender worker starting", "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.ProcessBatch(ctx); err != nil {
				slog.Error("Postback processing batch failed", "error", err)
			}
		}
	}
}

func (w *PostbackWorker) ProcessBatch(ctx context.Context) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	q := db.New(tx)
	events, err := q.GetPendingPostbackEventsForUpdate(ctx, 50)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}

	if len(events) == 0 {
		return nil
	}

	eventIDs := make([]int64, len(events))
	for i, ev := range events {
		eventIDs[i] = ev.ID
	}

	_, err = tx.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PROCESSING', processing_started_at = NOW()
		WHERE id = ANY($1)`, eventIDs)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	for _, ev := range events {
		err := w.ProcessEvent(ctx, ev)
		if err != nil {
			slog.Warn("Failed to process postback event", "id", ev.ID, "error", err)
			if errors.Is(err, ErrNotProTier) || errors.Is(err, ErrDuplicateEvent) {
				_, _ = w.pool.Exec(ctx, "UPDATE outbox_events SET status = 'FAILED', processing_started_at = NULL WHERE id = $1", ev.ID)
			} else {
				// Retry or DLQ is handled inside ProcessEvent, but we ensure outbox status is updated
				_, _ = w.pool.Exec(ctx, "UPDATE outbox_events SET status = 'FAILED', processing_started_at = NULL WHERE id = $1", ev.ID)
			}
		} else {
			_, _ = w.pool.Exec(ctx, "UPDATE outbox_events SET status = 'PROCESSED', processing_started_at = NULL WHERE id = $1", ev.ID)
		}
	}

	return nil
}

func (w *PostbackWorker) ProcessEvent(ctx context.Context, ev db.OutboxEvent) error {
	var payload PostbackPayload
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	// 1. Check Subscriptions Pro gate
	allowed, err := w.checkProTier(ctx, payload.CustomerID)
	if err != nil {
		return fmt.Errorf("failed to check subscription: %w", err)
	}
	if !allowed {
		return ErrNotProTier
	}

	// 2. Compute Idempotency hash: SHA256(customer_id|click_id|event_type)
	idempotencyStr := fmt.Sprintf("%s|%s|%s", payload.CustomerID, payload.ClickID, payload.EventType)
	hashBytes := sha256.Sum256([]byte(idempotencyStr))
	idempotencyHash := hex.EncodeToString(hashBytes[:])

	// Check if already dispatched
	var isDuplicate bool
	err = w.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM postback_dispatches WHERE idempotency_hash = $1)", idempotencyHash).Scan(&isDuplicate)
	if err != nil {
		return fmt.Errorf("failed to check idempotency: %w", err)
	}
	if isDuplicate {
		return ErrDuplicateEvent
	}

	// 3. Fetch Postback Configuration
	var config db.PostbackConfig
	q := db.New(w.pool)
	config, err = q.GetPostbackConfig(ctx, pgtype.UUID{Bytes: payload.CampaignID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("No postback config found for campaign, marking processed", "campaign_id", payload.CampaignID)
			return nil
		}
		return fmt.Errorf("failed to get postback config: %w", err)
	}

	// 4. Decrypt OAuth tokens / API keys
	var apiTokenDecrypted string
	if len(config.ApiTokenEncrypted) > 0 {
		decrypted, err := DecryptAESGCM(config.ApiTokenEncrypted, w.encryptionKey)
		if err != nil {
			return fmt.Errorf("failed to decrypt API token: %w", err)
		}
		apiTokenDecrypted = string(decrypted)
	}

	// 5. Get Rate Limiter for domain/provider
	limiter := w.getLimiter(config.UrlTemplate, config.Provider)
	if err := limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter wait aborted: %w", err)
	}

	// 6. Execute Dispatch with 5 Retries + Jitter
	adapter, ok := w.adapters[strings.ToLower(config.Provider)]
	if !ok {
		return fmt.Errorf("unsupported provider: %s", config.Provider)
	}

	if w.onDispatchAttempt != nil {
		w.onDispatchAttempt()
	}

	err = w.dispatchWithRetry(ctx, adapter, &payload, config.UrlTemplate, apiTokenDecrypted)
	if err != nil {
		// DLQ Insertion after 5 failures
		slog.Error("Postback dispatch failed completely, moving to DLQ", "error", err, "payload", payload)
		_, dlqErr := q.InsertPostbackDLQ(ctx, db.InsertPostbackDLQParams{
			OutboxEventID: ev.ID,
			CampaignID:    pgtype.UUID{Bytes: payload.CampaignID, Valid: true},
			ClickID:       payload.ClickID,
			EventType:     payload.EventType,
			Payload:       ev.Payload,
			FailuresCount: 5,
			LastError:     pgtype.Text{String: err.Error(), Valid: true},
			Status:        "FAILED",
		})
		if dlqErr != nil {
			slog.Error("Failed to insert into DLQ", "error", dlqErr)
			return fmt.Errorf("original error: %w; dlq insert failed: %s", err, dlqErr)
		}
		return fmt.Errorf("dispatch failed (moved to DLQ): %w", err)
	}

	// 7. Save to Postback Dispatches on Success (Idempotency)
	err = q.InsertPostbackDispatch(ctx, db.InsertPostbackDispatchParams{
		IdempotencyHash: idempotencyHash,
		CampaignID:      pgtype.UUID{Bytes: payload.CampaignID, Valid: true},
		ClickID:         payload.ClickID,
		EventType:       payload.EventType,
		Status:          "SENT",
	})
	if err != nil {
		slog.Error("Failed to record dispatch success", "error", err)
	}

	return nil
}

func (w *PostbackWorker) dispatchWithRetry(ctx context.Context, adapter PostbackAdapter, payload *PostbackPayload, urlTemplate, token string) error {
	var lastErr error
	maxRetries := 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter
			backoff := time.Duration(math.Pow(2, float64(attempt))) * 200 * time.Millisecond
			jitter := time.Duration(randInt64(50)) * time.Millisecond
			sleepTime := backoff + jitter

			slog.Info("Retrying postback dispatch", "attempt", attempt, "sleep", sleepTime)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleepTime):
			}
		}

		err := adapter.Send(ctx, w.client, payload, urlTemplate, token)
		if err == nil {
			return nil
		}
		lastErr = err

		// In chaos testing, we may abort early or continue based on failure type.
		// For transient errors, we continue retrying.
		slog.Warn("Postback dispatch attempt failed", "attempt", attempt+1, "error", err)
	}

	return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

func (w *PostbackWorker) checkProTier(ctx context.Context, customerID uuid.UUID) (bool, error) {
	var planCode string
	err := w.pool.QueryRow(ctx, "SELECT plan_code FROM billing.customer_subscriptions WHERE customer_id = $1", customerID).Scan(&planCode)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return strings.ToLower(planCode) == "pro" || strings.ToLower(planCode) == "enterprise", nil
}

func (w *PostbackWorker) getLimiter(targetURL string, provider string) *rate.Limiter {
	key := provider
	if u, err := url.Parse(targetURL); err == nil && u.Host != "" {
		key = u.Host
	}

	w.limitersMu.Lock()
	defer w.limitersMu.Unlock()

	lim, exists := w.limiters[key]
	if !exists {
		// Enforce a sensible default: 100 rps with 200 burst per destination
		lim = rate.NewLimiter(rate.Limit(100), 200)
		w.limiters[key] = lim
	}
	return lim
}

func randInt64(max int64) int64 {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	val := int64(b[0]) | int64(b[1])<<8 | int64(b[2])<<16 | int64(b[3])<<24 | int64(b[4])<<32 | int64(b[5])<<40 | int64(b[6])<<48 | int64(b[7])<<56
	if val < 0 {
		val = -val
	}
	return val % max
}

// AES-GCM Encrypt/Decrypt helpers

func EncryptAESGCM(plaintext []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func DecryptAESGCM(ciphertext []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, actualCiphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, actualCiphertext, nil)
}
type PostbackAdapter interface {
	Send(ctx context.Context, client *http.Client, payload *PostbackPayload, urlTemplate string, apiTokenDecrypted string) error
}
