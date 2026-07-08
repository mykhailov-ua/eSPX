package management

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/pkg/cold"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
)

// CreatePrivacyErasureRequest enqueues a GDPR-style erasure for async processing (M6.4).
func (s *Service) CreatePrivacyErasureRequest(ctx context.Context, userID string) (uuid.UUID, error) {
	if userID == "" {
		return uuid.Nil, errValidation("user_id is required")
	}
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	hash := ads.HashUserID(userID)
	_, err = db.New(s.GetPool()).CreatePrivacyErasureRequest(ctx, db.CreatePrivacyErasureRequestParams{
		ID:            ads.ToUUID(id),
		UserIDHash:    hash,
		SubjectUserID: userID,
	})
	return id, err
}

// ProcessPrivacyErasureTick advances in-flight erasure requests through the state machine.
func (s *Service) ProcessPrivacyErasureTick(ctx context.Context) error {
	opCtx, cancel := workerContext(ctx, workerBatchTimeout)
	defer cancel()

	q := db.New(s.GetPool())
	rows, err := q.ListPrivacyErasureRequestsByStatus(opCtx, db.ListPrivacyErasureRequestsByStatusParams{
		Status: db.PrivacyErasureStatusPENDING,
		Limit:  20,
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := s.advanceErasurePG(opCtx, row); err != nil {
			slog.Error("erasure PG step failed", "request_id", uuid.UUID(row.ID.Bytes), "error", err)
		}
	}

	rows, err = q.ListPrivacyErasureRequestsByStatus(opCtx, db.ListPrivacyErasureRequestsByStatusParams{
		Status: db.PrivacyErasureStatusPGANONYMIZED,
		Limit:  20,
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := s.enqueueErasureRedisPurge(opCtx, row); err != nil {
			slog.Error("erasure redis enqueue failed", "request_id", uuid.UUID(row.ID.Bytes), "error", err)
		}
	}

	rows, err = q.ListPrivacyErasureRequestsByStatus(opCtx, db.ListPrivacyErasureRequestsByStatusParams{
		Status: db.PrivacyErasureStatusREDISPURGED,
		Limit:  20,
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := s.advanceErasureCH(opCtx, row); err != nil {
			slog.Error("erasure CH step failed", "request_id", uuid.UUID(row.ID.Bytes), "error", err)
		}
	}
	return nil
}

func (s *Service) advanceErasurePG(ctx context.Context, row db.PrivacyErasureRequest) error {
	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		locked, err := q.GetPrivacyErasureRequestForUpdate(ctx, row.ID)
		if err != nil {
			return err
		}
		if locked.Status != db.PrivacyErasureStatusPENDING {
			return nil
		}
		if locked.SubjectUserID != "" {
			if err := q.AnonymizeEventsByUserID(ctx, pgtype.Text{String: locked.SubjectUserID, Valid: true}); err != nil {
				return err
			}
		}
		if err := q.AnonymizeConsentEventsByUserHash(ctx, locked.UserIDHash); err != nil {
			return err
		}
		if err := q.DeleteUserConsentState(ctx, locked.UserIDHash); err != nil {
			return err
		}
		return q.UpdatePrivacyErasureStatus(ctx, db.UpdatePrivacyErasureStatusParams{
			ID:     locked.ID,
			Status: db.PrivacyErasureStatusPGANONYMIZED,
		})
	})
}

func (s *Service) enqueueErasureRedisPurge(ctx context.Context, row db.PrivacyErasureRequest) error {
	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		locked, err := q.GetPrivacyErasureRequestForUpdate(ctx, row.ID)
		if err != nil {
			return err
		}
		if locked.Status != db.PrivacyErasureStatusPGANONYMIZED {
			return nil
		}
		if locked.LastError.Valid && locked.LastError.String == "purge_enqueued" {
			return nil
		}
		payload, err := cold.MarshalJSON(map[string]string{
			"erasure_id":      uuid.UUID(locked.ID.Bytes).String(),
			"user_id_hash":    hex.EncodeToString(locked.UserIDHash),
			"subject_user_id": locked.SubjectUserID,
		})
		if err != nil {
			return err
		}
		if _, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "PURGE_USER_DATA",
			Payload:   payload,
		}); err != nil {
			return err
		}
		return q.UpdatePrivacyErasureStatus(ctx, db.UpdatePrivacyErasureStatusParams{
			ID:        locked.ID,
			Status:    db.PrivacyErasureStatusPGANONYMIZED,
			LastError: pgtype.Text{String: "purge_enqueued", Valid: true},
		})
	})
}

func (s *Service) advanceErasureCH(ctx context.Context, row db.PrivacyErasureRequest) error {
	userID := row.SubjectUserID
	if s.ch != nil && userID != "" {
		query := `ALTER TABLE fraud_events DELETE WHERE user_id = ?`
		if err := s.ch.Exec(ctx, query, userID); err != nil {
			return s.failErasure(ctx, row.ID, err)
		}
	}
	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		if err := q.UpdatePrivacyErasureStatus(ctx, db.UpdatePrivacyErasureStatusParams{
			ID:     row.ID,
			Status: db.PrivacyErasureStatusCHPURGED,
		}); err != nil {
			return err
		}
		if err := q.UpdatePrivacyErasureStatus(ctx, db.UpdatePrivacyErasureStatusParams{
			ID:     row.ID,
			Status: db.PrivacyErasureStatusCOMPLETED,
		}); err != nil {
			return err
		}
		return q.ClearErasureSubjectUserID(ctx, row.ID)
	})
}

func (s *Service) failErasure(ctx context.Context, id pgtype.UUID, err error) error {
	msg := err.Error()
	return db.New(s.GetPool()).UpdatePrivacyErasureStatus(ctx, db.UpdatePrivacyErasureStatusParams{
		ID:        id,
		Status:    db.PrivacyErasureStatusFAILED,
		LastError: pgtype.Text{String: msg, Valid: true},
	})
}

// PurgeUserDataRedis deletes consent and fcap keys for a user on all shards (M6.4 outbox handler).
func (s *Service) PurgeUserDataRedis(ctx context.Context, hashHex, subjectUserID string) error {
	if len(s.rdbs) == 0 {
		return fmt.Errorf("no redis clients")
	}
	consentKey := ads.ConsentRedisKeyPrefix + hashHex
	pattern := "*:u:" + subjectUserID
	var firstErr error
	var success int
	for _, rdb := range s.rdbs {
		if err := rdb.Del(ctx, consentKey).Err(); err != nil && err != redis.Nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		success++
		iter := rdb.Scan(ctx, 0, pattern, 200).Iterator()
		for iter.Next(ctx) {
			_ = rdb.Del(ctx, iter.Val()).Err()
		}
		_ = iter.Err()
	}
	if success == 0 && firstErr != nil {
		return firstErr
	}
	channel := s.consentUpdateChannel()
	if pub := s.getPubSubRDB(); pub != nil {
		_ = pub.Publish(ctx, channel, hashHex).Err()
	}
	return nil
}

// SyncUserConsentToRedis writes consent purposes to every shard and publishes an update (M6.2).
func (s *Service) SyncUserConsentToRedis(ctx context.Context, hashHex string, purposes int16) error {
	if len(s.rdbs) == 0 {
		return fmt.Errorf("no redis clients")
	}
	val := strconv.FormatInt(int64(purposes), 10)
	key := ads.ConsentRedisKeyPrefix + hashHex
	for _, rdb := range s.rdbs {
		if err := rdb.Set(ctx, key, val, 0).Err(); err != nil {
			return err
		}
	}
	if pub := s.getPubSubRDB(); pub != nil {
		return pub.Publish(ctx, s.consentUpdateChannel(), hashHex).Err()
	}
	return nil
}

func (s *Service) consentUpdateChannel() string {
	if s.cfg != nil && s.cfg.ConsentUpdateChannel != "" {
		return s.cfg.ConsentUpdateChannel
	}
	return ads.ConsentDefaultUpdateChannel
}

// MarkErasureRedisPurgeDone advances erasure after Redis purge outbox (M6.4).
func (s *Service) MarkErasureRedisPurgeDone(ctx context.Context, erasureID uuid.UUID, partialErr error) error {
	status := db.PrivacyErasureStatusREDISPURGED
	if partialErr != nil {
		return db.New(s.GetPool()).UpdatePrivacyErasureStatus(ctx, db.UpdatePrivacyErasureStatusParams{
			ID:        ads.ToUUID(erasureID),
			Status:    db.PrivacyErasureStatusFAILED,
			LastError: pgtype.Text{String: partialErr.Error(), Valid: true},
		})
	}
	return db.New(s.GetPool()).UpdatePrivacyErasureStatus(ctx, db.UpdatePrivacyErasureStatusParams{
		ID:     ads.ToUUID(erasureID),
		Status: status,
	})
}
