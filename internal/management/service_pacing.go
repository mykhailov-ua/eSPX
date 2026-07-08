package management

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/pkg/cold"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ClosedLoopPacingController switches campaigns between ASAP and EVEN when spend diverges from the daypart curve.
func (s *Service) ClosedLoopPacingController(ctx context.Context, syncWorkers []*ads.SyncWorker) error {
	opCtx, cancel := workerContext(ctx, workerBatchTimeout)
	defer cancel()

	for _, sw := range syncWorkers {
		if sw != nil {
			sw.SyncAll(opCtx)
		}
	}

	return pgx.BeginFunc(opCtx, s.GetPool(), func(tx pgx.Tx) error {
		return s.closedLoopPacingControllerTx(opCtx, tx, nil)
	})
}

func (s *Service) closedLoopPacingControllerTx(ctx context.Context, tx pgx.Tx, merge deliveryOutboxMerge) error {
	q := db.New(tx)
	rows, err := q.GetAllActiveCampaignsWithStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch active campaigns for pacing: %w", err)
	}

	hourWeights := s.fetchPacingHourWeights(ctx)
	now := time.Now()

	for _, row := range rows {
		camp, err := q.GetCampaignForUpdate(ctx, row.ID)
		if err != nil {
			return fmt.Errorf("failed to lock campaign for pacing: %w", err)
		}
		if camp.Status != db.CampaignStatusTypeACTIVE {
			continue
		}

		loc := s.campaignLocation(camp.Timezone)
		localNow := now.In(loc)

		daypart := camp.DaypartHours
		if daypart == nil {
			daypart = []int16{}
		}
		timeRatio := smartPacingExpectedRatio(hourWeights, daypart, localNow)

		budgetMicro := camp.DailyBudget
		if budgetMicro == 0 {
			budgetMicro = camp.BudgetLimit
		}
		if budgetMicro == 0 {
			continue
		}

		actualSpendMicro := camp.CurrentSpend
		expectedSpendMicro := int64(float64(budgetMicro) * timeRatio)

		var targetPacing db.PacingModeType
		var shouldUpdate bool

		overThresholdMicro := int64(float64(expectedSpendMicro) * (1.0 + s.cfg.PacingToleranceMargin))
		underThresholdMicro := int64(float64(expectedSpendMicro) * (1.0 - s.cfg.PacingToleranceMargin))

		if camp.PacingMode == db.PacingModeTypeASAP && actualSpendMicro > overThresholdMicro {
			targetPacing = db.PacingModeTypeEVEN
			shouldUpdate = true
		} else if camp.PacingMode == db.PacingModeTypeEVEN && actualSpendMicro < underThresholdMicro {
			targetPacing = db.PacingModeTypeASAP
			shouldUpdate = true
		}

		if !shouldUpdate {
			continue
		}

		campID := uuid.UUID(camp.ID.Bytes)
		_, err = q.UpdateCampaignPacing(ctx, db.UpdateCampaignPacingParams{
			ID:         camp.ID,
			PacingMode: targetPacing,
		})
		if err != nil {
			return fmt.Errorf("failed to update pacing mode: %w", err)
		}

		actualSpendStr := fmt.Sprintf("%.2f", float64(actualSpendMicro)/1_000_000.0)
		expectedSpendStr := fmt.Sprintf("%.2f", float64(expectedSpendMicro)/1_000_000.0)

		s.AuditLog(ctx, q, uuid.Nil, "PACING_LOOP_ADJUSTMENT", "campaign", &campID, map[string]any{
			"old_pacing": string(camp.PacingMode),
			"new_pacing": string(targetPacing),
			"spend":      actualSpendStr,
			"expected":   expectedSpendStr,
			"curve":      "daypart_weighted",
		}, nil)

		payloadBytes, err := cold.MarshalJSON(map[string]any{
			"campaign_id": campID.String(),
			"pacing_mode": string(targetPacing),
		})
		if err != nil {
			return fmt.Errorf("failed to marshal pacing outbox payload: %w", err)
		}

		if merge != nil {
			merge.upsert(campID, outboxPriPacing, "UPDATE_CAMPAIGN_PACING", payloadBytes)
		} else {
			_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
				EventType: "UPDATE_CAMPAIGN_PACING",
				Payload:   payloadBytes,
			})
			if err != nil {
				return fmt.Errorf("failed to create outbox event for pacing: %w", err)
			}
		}

		slog.Info("closed-loop pacing controller adjusted pacing",
			"campaign_id", campID,
			"old_pacing", camp.PacingMode,
			"new_pacing", targetPacing,
			"actual_spend", actualSpendStr,
			"expected_spend", expectedSpendStr,
		)
	}

	return nil
}

func (s *Service) campaignLocation(timezone string) *time.Location {
	if cached, found := s.locCache.Load(timezone); found {
		return cached.(*time.Location)
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	s.locCache.Store(timezone, loc)
	return loc
}
