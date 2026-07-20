package marginguard

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"
	notifierpb "espx/internal/notifier/pb"
	"espx/pkg/money"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Notifier interface {
	SendNotification(ctx context.Context, provider notifierpb.Provider, recipient, title, body string) (*notifierpb.SendNotificationResponse, error)
}

type Worker struct {
	pool     *pgxpool.Pool
	ch       *database.CHQuery
	cfg      *config.Config
	registry *ingestion.Registry
	notifier Notifier
}

func NewWorker(pool *pgxpool.Pool, ch *database.CHQuery, cfg *config.Config, registry *ingestion.Registry, notifier Notifier) *Worker {
	return &Worker{
		pool:     pool,
		ch:       ch,
		cfg:      cfg,
		registry: registry,
		notifier: notifier,
	}
}

type PausePlacementPayload struct {
	CampaignID  string `json:"campaign_id"`
	PlacementID string `json:"placement_id"`
}

func (w *Worker) Start(ctx context.Context, interval time.Duration) {
	slog.Info("margin guard worker starting", "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.RunCycle(ctx); err != nil {
				slog.Error("margin guard cycle failed", "error", err)
			}
		}
	}
}

func (w *Worker) RunCycle(ctx context.Context) error {
	// 1. Check ClickHouse lag
	var lag int64
	err := w.ch.QueryRow(ctx, "SELECT dateDiff('second', max(hour), now()) FROM placement_stats_hourly").Scan(&lag)
	if err != nil {
		// Fallback to cost_snapshots if placement_stats_hourly is empty
		_ = w.ch.QueryRow(ctx, "SELECT dateDiff('second', max(snapshot_hour), now()) FROM cost_snapshots").Scan(&lag)
	}

	if lag > 300 {
		slog.Warn("margin guard skipping cycle due to ch lag", "lag_seconds", lag)
		return nil
	}

	// 2. Fetch active policies
	policies, err := w.fetchActivePolicies(ctx)
	if err != nil {
		return fmt.Errorf("fetch policies: %w", err)
	}

	for _, policy := range policies {
		if err := w.evaluatePolicy(ctx, policy); err != nil {
			slog.Error("failed to evaluate policy", "policy_id", policy.ID, "campaign_id", policy.CampaignID, "error", err)
		}
	}

	return nil
}

func (w *Worker) fetchActivePolicies(ctx context.Context) ([]*Policy, error) {
	rows, err := w.pool.Query(ctx, "SELECT id, campaign_id, name, min_clicks, roi_floor_pct, zero_conv_streak, is_active FROM margin_guard_policies WHERE is_active = true")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var policies []*Policy
	for rows.Next() {
		p := &Policy{}
		if err := rows.Scan(&p.ID, &p.CampaignID, &p.Name, &p.MinClicks, &p.RoiFloorPct, &p.ZeroConvStreak, &p.IsActive); err != nil {
			return nil, err
		}
		policies = append(policies, p)
	}
	return policies, nil
}

func (w *Worker) evaluatePolicy(ctx context.Context, policy *Policy) error {
	// 0. Pro Tier check
	if w.registry != nil {
		camp, ok := w.registry.GetCampaign(policy.CampaignID)
		if ok {
			ent, ok := w.registry.GetEntitlements(camp.CustomerID)
			if ok && !ent.Features.MarginGuard {
				slog.Warn("skipping policy evaluation: customer not entitled to margin guard", "customer_id", camp.CustomerID, "policy_id", policy.ID)
				return nil
			}
		}
	}

	// Query ClickHouse for placement stats for this campaign in the last 24h
	query := `
		SELECT 
			placement_id, 
			sum(spend_micro) as spend, 
			sum(revenue_micro) as revenue,
			sum(click_count) as clicks,
			sum(conversion_count) as conversions
		FROM placement_stats_hourly
		WHERE campaign_id = ?
		  AND hour >= now() - INTERVAL 24 HOUR
		GROUP BY placement_id
	`

	rows, err := w.ch.Query(ctx, query, policy.CampaignID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var stats PlacementStats
		stats.CampaignID = policy.CampaignID
		if err := rows.Scan(&stats.PlacementID, &stats.SpendMicro, &stats.RevenueMicro, &stats.Clicks, &stats.Conversions); err != nil {
			return err
		}

		if decision, trigger := Evaluate(policy, &stats); trigger {
			if err := w.applyDecision(ctx, decision); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Worker) applyDecision(ctx context.Context, d *Decision) error {
	// 1. Check if already paused (avoid duplicate outbox/activity)
	var exists bool
	err := w.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM margin_guard_activity WHERE campaign_id = $1 AND placement_id = $2 AND action = 'pause' AND created_at > now() - INTERVAL '1 day')", d.CampaignID, d.PlacementID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// 2. Record activity
	metricsJSON, _ := json.Marshal(d.Metrics)
	_, err = w.pool.Exec(ctx, `
		INSERT INTO margin_guard_activity (policy_id, campaign_id, placement_id, action, reason, metrics)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, d.PolicyID, d.CampaignID, d.PlacementID, d.Action, d.Reason, metricsJSON)
	if err != nil {
		return err
	}

	// 3. Enqueue outbox event
	if d.Action == ActionPause {
		payload, _ := json.Marshal(PausePlacementPayload{
			CampaignID:  d.CampaignID.String(),
			PlacementID: d.PlacementID,
		})
		_, err = w.pool.Exec(ctx, "INSERT INTO outbox_events (event_type, payload) VALUES ($1, $2)", "PAUSE_PLACEMENT", payload)
		if err != nil {
			return err
		}

		// 4. Send alert via Notifier
		if w.notifier != nil {
			title := fmt.Sprintf("Margin Guard: Placement Paused (%s)", d.PlacementID)
			body := fmt.Sprintf("Campaign: %s\nPlacement: %s\nReason: %s\nROI: %.2f%%\nSpend: %s USD\nRevenue: %s USD\nClicks: %d\nConversions: %d",
				d.CampaignID, d.PlacementID, d.Reason, d.Metrics["roi_pct"],
				money.FormatDecimal(d.Metrics["spend_micro"].(int64)),
				money.FormatDecimal(d.Metrics["revenue_micro"].(int64)),
				d.Metrics["clicks"], d.Metrics["conversions"])

			_, alertErr := w.notifier.SendNotification(ctx, notifierpb.Provider_PROVIDER_TELEGRAM, "admin", title, body)
			if alertErr != nil {
				slog.Error("failed to send margin guard notification", "error", alertErr)
			}
		}
	}

	slog.Info("margin guard applied decision", "campaign_id", d.CampaignID, "placement_id", d.PlacementID, "action", d.Action, "reason", d.Reason)
	return nil
}
