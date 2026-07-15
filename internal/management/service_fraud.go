package management

import (
	"context"
	"fmt"

	"espx/internal/campaignmodel"
	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"
	"espx/pkg/coldpath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CampaignFraudConfigDTO is the admin API view of per-campaign fraud scoring and behavior toggles.
type CampaignFraudConfigDTO struct {
	CampaignID            string `json:"campaign_id"`
	FraudThresholdPass    uint8  `json:"fraud_threshold_pass"`
	FraudThresholdSuspect uint8  `json:"fraud_threshold_suspect"`
	FraudThresholdIVT     uint8  `json:"fraud_threshold_ivt"`
	FraudThresholdBlock   uint8  `json:"fraud_threshold_block"`
	GhostIVTEnabled       bool   `json:"ghost_ivt_enabled"`
	BehaviorFlags         uint32 `json:"behavior_flags"`
}

// CampaignFraudConfigUpdate is the request body for POST /admin/campaigns/{id}/fraud-config.
type CampaignFraudConfigUpdate struct {
	FraudThresholdPass    *uint8  `json:"fraud_threshold_pass,omitempty"`
	FraudThresholdSuspect *uint8  `json:"fraud_threshold_suspect,omitempty"`
	FraudThresholdIVT     *uint8  `json:"fraud_threshold_ivt,omitempty"`
	FraudThresholdBlock   *uint8  `json:"fraud_threshold_block,omitempty"`
	GhostIVTEnabled       *bool   `json:"ghost_ivt_enabled,omitempty"`
	BehaviorFlags         *uint32 `json:"behavior_flags,omitempty"`
}

func campaignFraudConfigFromRow(id uuid.UUID, row db.Campaign) CampaignFraudConfigDTO {
	return CampaignFraudConfigDTO{
		CampaignID:            id.String(),
		FraudThresholdPass:    uint8(row.FraudThresholdPass),
		FraudThresholdSuspect: uint8(row.FraudThresholdSuspect),
		FraudThresholdIVT:     uint8(row.FraudThresholdIvt),
		FraudThresholdBlock:   uint8(row.FraudThresholdBlock),
		GhostIVTEnabled:       row.GhostIvtEnabled,
		BehaviorFlags:         uint32(row.BehaviorFlags),
	}
}

func validateFraudThresholds(pass, suspect, ivt, block uint8) error {
	if pass > 100 || suspect > 100 || ivt > 100 || block > 100 {
		return errValidation("fraud thresholds must be between 0 and 100")
	}
	if pass > suspect || suspect > ivt || ivt > block {
		return errValidation("fraud thresholds must be ordered: pass <= suspect <= ivt <= block")
	}
	return nil
}

// GetCampaignFraudConfig returns the current fraud configuration for a campaign.
func (s *Service) GetCampaignFraudConfig(ctx context.Context, campaignID uuid.UUID) (CampaignFraudConfigDTO, error) {
	row, err := db.New(s.GetPool()).GetCampaignFull(ctx, ingestion.ToUUID(campaignID))
	if err != nil {
		return CampaignFraudConfigDTO{}, mapNotFound(err, ErrCampaignNotFound)
	}
	return campaignFraudConfigFromRow(campaignID, row), nil
}

// UpdateCampaignFraudConfig persists fraud settings and notifies trackers via the outbox pub/sub path.
func (s *Service) UpdateCampaignFraudConfig(ctx context.Context, campaignID uuid.UUID, upd CampaignFraudConfigUpdate) (CampaignFraudConfigDTO, error) {
	var out CampaignFraudConfigDTO

	err := pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		locked, err := q.GetCampaignForUpdate(ctx, ingestion.ToUUID(campaignID))
		if err != nil {
			return mapNotFound(err, ErrCampaignNotFound)
		}

		pass := uint8(locked.FraudThresholdPass)
		suspect := uint8(locked.FraudThresholdSuspect)
		ivt := uint8(locked.FraudThresholdIvt)
		block := uint8(locked.FraudThresholdBlock)
		ghost := locked.GhostIvtEnabled
		flags := locked.BehaviorFlags

		if upd.FraudThresholdPass != nil {
			pass = *upd.FraudThresholdPass
		}
		if upd.FraudThresholdSuspect != nil {
			suspect = *upd.FraudThresholdSuspect
		}
		if upd.FraudThresholdIVT != nil {
			ivt = *upd.FraudThresholdIVT
		}
		if upd.FraudThresholdBlock != nil {
			block = *upd.FraudThresholdBlock
		}
		if upd.GhostIVTEnabled != nil {
			ghost = *upd.GhostIVTEnabled
		}
		if upd.BehaviorFlags != nil {
			flags = int32(*upd.BehaviorFlags)
		}

		if err := validateFraudThresholds(pass, suspect, ivt, block); err != nil {
			return err
		}

		updated, err := q.UpdateCampaignFraudConfig(ctx, db.UpdateCampaignFraudConfigParams{
			ID:                    ingestion.ToUUID(campaignID),
			FraudThresholdPass:    int16(pass),
			FraudThresholdSuspect: int16(suspect),
			FraudThresholdIvt:     int16(ivt),
			FraudThresholdBlock:   int16(block),
			GhostIvtEnabled:       ghost,
			BehaviorFlags:         flags,
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "UPDATE_CAMPAIGN_FRAUD", "campaign", &campaignID, map[string]any{
			"fraud_threshold_pass":    pass,
			"fraud_threshold_suspect": suspect,
			"fraud_threshold_ivt":     ivt,
			"fraud_threshold_block":   block,
			"ghost_ivt_enabled":       ghost,
			"behavior_flags":          flags,
		}, nil)

		payload, err := coldpath.MarshalJSON(map[string]string{"campaign_id": campaignID.String()})
		if err != nil {
			return fmt.Errorf("marshal update campaign fraud outbox payload: %w", err)
		}
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "UPDATE_CAMPAIGN_FRAUD",
			Payload:   payload,
		})
		if err != nil {
			return err
		}

		out = campaignFraudConfigFromRow(campaignID, updated)
		return nil
	})
	if err != nil {
		return CampaignFraudConfigDTO{}, err
	}
	return out, nil
}

// ResolveFraudThresholds returns campaign thresholds or PLAN defaults when unset in storage.
func ResolveFraudThresholds(camp *campaignmodel.Campaign) (pass, suspect, ivt, block uint8) {
	if camp == nil {
		return campaignmodel.DefaultFraudThresholdPass, campaignmodel.DefaultFraudThresholdSuspect,
			campaignmodel.DefaultFraudThresholdIVT, campaignmodel.DefaultFraudThresholdBlock
	}
	return camp.FraudThresholdPass, camp.FraudThresholdSuspect, camp.FraudThresholdIVT, camp.FraudThresholdBlock
}

// FraudScoringOverrideRequest is the request payload for POST /admin/fraud-scoring/overrides.
type FraudScoringOverrideRequest struct {
	CampaignID *string `json:"campaign_id,omitempty"`
	IP         *string `json:"ip,omitempty"`
}

// ApplyFraudScoringOverride clears a campaign score boost and/or removes an IP from the fraud blacklist.
func (s *Service) ApplyFraudScoringOverride(ctx context.Context, req FraudScoringOverrideRequest) error {
	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}

		if req.CampaignID != nil && *req.CampaignID != "" {
			campUUID, err := uuid.Parse(*req.CampaignID)
			if err != nil {
				return errValidation("invalid campaign_id format")
			}

			s.AuditLog(ctx, q, uid, "FRAUD_CLEAR_BOOST", "campaign", &campUUID, map[string]string{"campaign_id": *req.CampaignID}, nil)

			payload, err := coldpath.MarshalJSON(FraudThreatPayload{
				Action:     "boost",
				CampaignID: *req.CampaignID,
				Boost:      0,
				TTLSeconds: 0,
			})
			if err != nil {
				return err
			}
			_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
				EventType: "ML_SCORE_BOOST",
				Payload:   payload,
			})
			if err != nil {
				return err
			}
		}

		if req.IP != nil && *req.IP != "" {
			err := q.DeleteBlacklistIP(ctx, *req.IP)
			if err != nil {
				return err
			}

			s.AuditLog(ctx, q, uid, "FRAUD_REMOVE_FALSE_POSITIVE", "system", nil, map[string]string{"ip": *req.IP}, nil)

			payload, err := coldpath.MarshalJSON(BlacklistPayload{Action: "remove", IP: *req.IP, Reason: "fraud"})
			if err != nil {
				return fmt.Errorf("marshal blacklist outbox payload: %w", err)
			}
			_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
				EventType: "UPDATE_BLACKLIST",
				Payload:   payload,
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}
