package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/shopspring/decimal"
)

// AutoscaleBudgets coordinates budget shifts from low CTR to high CTR campaigns under the same customer.
// It executes a synchronous SyncAll over all SyncWorkers to ensure Postgres data is fully current.
func (s *Service) AutoscaleBudgets(ctx context.Context, syncWorkers []*ads.SyncWorker) error {
	for _, sw := range syncWorkers {
		sw.SyncAll(ctx)
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		rows, err := q.GetAllActiveCampaignsWithStats(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch active campaigns with stats: %w", err)
		}

		byCustomer := make(map[uuid.UUID][]db.GetAllActiveCampaignsWithStatsRow)
		for _, row := range rows {
			custID := uuid.UUID(row.CustomerID.Bytes)
			byCustomer[custID] = append(byCustomer[custID], row)
		}

		for custID, campaigns := range byCustomer {
			if len(campaigns) < 2 {
				continue
			}

			var bestCamp *db.GetAllActiveCampaignsWithStatsRow
			var bestCTR float64 = -1.0

			var worstCamp *db.GetAllActiveCampaignsWithStatsRow
			var worstCTR float64 = 2.0

			for i := range campaigns {
				c := &campaigns[i]
				if c.TotalImpressions <= 0 {
					continue
				}
				ctr := float64(c.TotalClicks) / float64(c.TotalImpressions)

				if ctr > 0.015 && c.TotalImpressions > 100 {
					if ctr > bestCTR {
						bestCTR = ctr
						bestCamp = c
					}
				}

				limit := ads.FromNumeric(c.BudgetLimit)
				spend := ads.FromNumeric(c.CurrentSpend)
				remaining := limit.Sub(spend)

				if ctr < 0.005 && remaining.GreaterThanOrEqual(decimal.NewFromInt(20)) {
					if ctr < worstCTR {
						worstCTR = ctr
						worstCamp = c
					}
				}
			}

			if bestCamp != nil && worstCamp != nil {
				bestID := uuid.UUID(bestCamp.ID.Bytes)
				worstID := uuid.UUID(worstCamp.ID.Bytes)

				if bestID == worstID {
					continue
				}

				shiftAmount := decimal.NewFromInt(10)
				worstLimit := ads.FromNumeric(worstCamp.BudgetLimit)
				bestLimit := ads.FromNumeric(bestCamp.BudgetLimit)

				newWorstLimit := worstLimit.Sub(shiftAmount)
				newBestLimit := bestLimit.Add(shiftAmount)

				_, err = q.UpdateCampaignBudget(ctx, db.UpdateCampaignBudgetParams{
					ID:          ads.ToUUID(worstID),
					BudgetLimit: ads.ToNumeric(newWorstLimit),
				})
				if err != nil {
					return fmt.Errorf("failed to decrease budget for campaign %s: %w", worstID, err)
				}

				_, err = q.UpdateCampaignBudget(ctx, db.UpdateCampaignBudgetParams{
					ID:          ads.ToUUID(bestID),
					BudgetLimit: ads.ToNumeric(newBestLimit),
				})
				if err != nil {
					return fmt.Errorf("failed to increase budget for campaign %s: %w", bestID, err)
				}

				s.AuditLog(ctx, q, uuid.Nil, "AUTOSCALE_BUDGET_TRANSFER", "campaign", &worstID, map[string]any{
					"old_budget": worstLimit.StringFixed(2),
					"new_budget": newWorstLimit.StringFixed(2),
					"ctr":        worstCTR,
					"target":     bestID.String(),
				}, nil)

				s.AuditLog(ctx, q, uuid.Nil, "AUTOSCALE_BUDGET_TRANSFER", "campaign", &bestID, map[string]any{
					"old_budget": bestLimit.StringFixed(2),
					"new_budget": newBestLimit.StringFixed(2),
					"ctr":        bestCTR,
					"source":     worstID.String(),
				}, nil)

				worstPayload, _ := json.Marshal(CampaignPayload{
					CampaignID:  worstID.String(),
					BudgetLimit: newWorstLimit.StringFixed(2),
				})
				bestPayload, _ := json.Marshal(CampaignPayload{
					CampaignID:  bestID.String(),
					BudgetLimit: newBestLimit.StringFixed(2),
				})

				_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
					EventType: "CREATE_CAMPAIGN",
					Payload:   worstPayload,
				})
				if err != nil {
					return fmt.Errorf("failed to create outbox event for worst campaign: %w", err)
				}

				_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
					EventType: "CREATE_CAMPAIGN",
					Payload:   bestPayload,
				})
				if err != nil {
					return fmt.Errorf("failed to create outbox event for best campaign: %w", err)
				}

				slog.Info("autoscaled budgets by rule",
					"customer_id", custID,
					"decreased_campaign", worstID,
					"decreased_ctr", worstCTR,
					"increased_campaign", bestID,
					"increased_ctr", bestCTR,
					"shift_amount", shiftAmount,
				)
			}
		}

		return nil
	})
}
