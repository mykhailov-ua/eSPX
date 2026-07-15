package management

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"
	"espx/pkg/coldpath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrConsentInvalidSignature = errors.New("invalid consent signature")
	ErrConsentInvalidPayload   = errors.New("invalid consent payload")
)

// ConsentRecordInput is the signed body for POST /api/v1/consent (M6.2).
type ConsentRecordInput struct {
	UserID    string `json:"user_id"`
	Purposes  int16  `json:"purposes"`
	Source    string `json:"source"`
	Timestamp string `json:"timestamp,omitempty"`
}

// RecordConsent persists consent, updates user flags, and enqueues Redis sync (M6.2).
func (s *Service) RecordConsent(ctx context.Context, in ConsentRecordInput) error {
	if in.UserID == "" {
		return errValidation("user_id is required")
	}
	if in.Source == "" {
		return errValidation("source is required")
	}
	if in.Purposes < 0 {
		return errValidation("purposes must be non-negative")
	}

	hash := ingestion.HashUserID(in.UserID)
	adStorage, analyticsStorage := ingestion.ConsentFlagsFromPurposes(in.Purposes)

	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		if err := q.InsertConsentEvent(ctx, db.InsertConsentEventParams{
			UserIDHash: hash,
			Purposes:   in.Purposes,
			Source:     in.Source,
		}); err != nil {
			return fmt.Errorf("insert consent event: %w", err)
		}
		if err := q.UpsertUserConsentState(ctx, db.UpsertUserConsentStateParams{
			UserIDHash:       hash,
			AdStorage:        adStorage,
			AnalyticsStorage: analyticsStorage,
			Purposes:         in.Purposes,
		}); err != nil {
			return fmt.Errorf("upsert consent state: %w", err)
		}
		payload, err := coldpath.MarshalJSON(map[string]any{
			"user_id_hash": hex.EncodeToString(hash),
			"purposes":     in.Purposes,
		})
		if err != nil {
			return fmt.Errorf("marshal consent outbox payload: %w", err)
		}
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "SYNC_USER_CONSENT",
			Payload:   payload,
		})
		return err
	})
}

// VerifyConsentHMAC validates X-Consent-Signature over the raw request body.
func VerifyConsentHMAC(secret []byte, body []byte, signatureHex string) error {
	if len(secret) == 0 {
		return ErrConsentInvalidSignature
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	expected := mac.Sum(nil)
	got, err := hex.DecodeString(signatureHex)
	if err != nil {
		return ErrConsentInvalidSignature
	}
	if !hmac.Equal(expected, got) {
		return ErrConsentInvalidSignature
	}
	return nil
}

// UpdateCampaignConsentRequirements sets require_consent_purposes and notifies trackers (M6.3).
func (s *Service) UpdateCampaignConsentRequirements(ctx context.Context, campaignID uuid.UUID, purposes int16) error {
	if purposes < 0 {
		return errValidation("require_consent_purposes must be non-negative")
	}
	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		if _, err := q.UpdateCampaignConsentPurposes(ctx, db.UpdateCampaignConsentPurposesParams{
			ID:                     ingestion.ToUUID(campaignID),
			RequireConsentPurposes: purposes,
		}); err != nil {
			return mapNotFound(err, ErrCampaignNotFound)
		}
		payload, err := coldpath.MarshalJSON(map[string]string{"campaign_id": campaignID.String()})
		if err != nil {
			return err
		}
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "UPDATE_CAMPAIGN_CONSENT",
			Payload:   payload,
		})
		return err
	})
}

// CleanupConsentEvents deletes consent audit rows older than retention (M6.1).
func (s *Service) CleanupConsentEvents(ctx context.Context) error {
	if s.cfg == nil || s.cfg.ConsentRetentionMonths <= 0 {
		return nil
	}
	threshold := time.Now().AddDate(0, -s.cfg.ConsentRetentionMonths, 0)
	return db.New(s.GetPool()).CleanupConsentEventsOlderThan(ctx, pgtype.Timestamptz{Time: threshold, Valid: true})
}
