package ivtdetector

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/mlanalytics"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type campaignFraudConfig struct {
	pass    uint8
	suspect uint8
	block   uint8
	ghost   bool
}

type mlRule struct {
	conn      driver.Conn
	pool      *pgxpool.Pool
	scorer    mlanalytics.Scorer
	batchSize int
}

// NewMLRule creates a new ML shadow scoring rule.
func NewMLRule(conn driver.Conn, pool *pgxpool.Pool, scorer mlanalytics.Scorer, batchSize int) Rule {
	return &mlRule{
		conn:      conn,
		pool:      pool,
		scorer:    scorer,
		batchSize: batchSize,
	}
}

func (r *mlRule) fetchCampaignConfigs(ctx context.Context) (map[string]campaignFraudConfig, error) {
	if r.pool == nil {
		return nil, nil
	}
	rows, err := r.pool.Query(ctx, "SELECT id, fraud_threshold_pass, fraud_threshold_suspect, fraud_threshold_block, ghost_ivt_enabled FROM campaigns")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	configs := make(map[string]campaignFraudConfig)
	for rows.Next() {
		var id uuid.UUID
		var pass, suspect, block int16
		var ghost bool
		if err := rows.Scan(&id, &pass, &suspect, &block, &ghost); err != nil {
			return nil, err
		}
		configs[id.String()] = campaignFraudConfig{
			pass:    uint8(pass),
			suspect: uint8(suspect),
			block:   uint8(block),
			ghost:   ghost,
		}
	}
	return configs, nil
}

func (r *mlRule) Name() string {
	return "ml_analytics_shadow"
}

func (r *mlRule) Find(ctx context.Context) ([]SuspiciousIP, error) {
	if r.conn == nil || r.scorer == nil {
		return nil, nil
	}

	// Fetch campaign configs from Postgres first
	configs, err := r.fetchCampaignConfigs(ctx)
	if err != nil {
		slog.Warn("ml shadow scoring: failed to fetch campaign configs from postgres", "error", err)
	}

	// Fetch recent features from ml_features_1m
	query := `
SELECT
    window_start,
    ip_address,
    campaign_id,
    events,
    clicks,
    spend_micro,
    budget_limit_micro,
    unique_users,
    unique_uas
FROM ml_features_1m
WHERE window_start >= now() - INTERVAL 5 MINUTE
ORDER BY window_start DESC
LIMIT ?`

	rows, err := r.conn.Query(ctx, query, r.batchSize)
	if err != nil {
		mlScoringErrorsTotal.Inc()
		slog.Warn("ml shadow scoring skipped: clickhouse query failed", "error", err)
		return nil, nil
	}
	defer rows.Close()

	var featureRows []mlanalytics.FeatureRow
	for rows.Next() {
		var fr mlanalytics.FeatureRow
		var campaignID string
		if err := rows.Scan(
			&fr.WindowStart,
			&fr.IPAddress,
			&campaignID,
			&fr.Events,
			&fr.Clicks,
			&fr.SpendMicro,
			&fr.BudgetLimitMicro,
			&fr.UniqueUsers,
			&fr.UniqueUAs,
		); err != nil {
			mlScoringErrorsTotal.Inc()
			slog.Warn("ml shadow scoring skipped: clickhouse scan failed", "error", err)
			return nil, nil
		}
		fr.CampaignID = campaignID
		featureRows = append(featureRows, fr)
	}

	if len(featureRows) == 0 {
		return nil, nil
	}

	mlCandidatesTotal.Add(float64(len(featureRows)))

	start := time.Now()
	scores, err := r.scorer.ScoreBatch(ctx, featureRows)
	duration := time.Since(start).Seconds()
	mlScoringDurationSeconds.Observe(duration)

	if err != nil {
		mlScoringErrorsTotal.Inc()
		slog.Warn("ml shadow scoring skipped: model inference failed", "error", err)
		return nil, nil
	}

	var out []SuspiciousIP
	for i, score := range scores {
		ip := featureRows[i].IPAddress
		slog.Info("ml shadow score",
			"ip", ip,
			"ml_shadow_score", score,
			"model", r.scorer.Name(),
		)

		insertQuery := `
INSERT INTO ml_shadow_scores (ip_address, score, model_name, created_at)
VALUES (?, ?, ?, ?)`
		if err := r.conn.Exec(ctx, insertQuery, ip, score, r.scorer.Name(), time.Now()); err != nil {
			slog.Error("failed to insert ml shadow score to clickhouse", "error", err, "ip", ip)
		}

		// Map probability to fraud score
		fraudScore := mlanalytics.ProbabilityToFraudScore(score)

		// Default thresholds
		pass := uint8(30)
		suspect := uint8(60)
		block := uint8(100)
		ghostEnabled := false

		if configs != nil {
			if cfg, ok := configs[featureRows[i].CampaignID]; ok {
				pass = cfg.pass
				suspect = cfg.suspect
				block = cfg.block
				ghostEnabled = cfg.ghost
			}
		}

		if uint8(fraudScore) >= pass && uint8(fraudScore) < suspect {
			out = append(out, SuspiciousIP{
				IP:         ip,
				Reason:     r.scorer.Name(),
				Score:      float64(fraudScore),
				CampaignID: featureRows[i].CampaignID,
				Action:     "boost",
				Boost:      int32(fraudScore),
				TTLSeconds: 300, // 5 minutes TTL
			})
		} else if uint8(fraudScore) >= suspect && uint8(fraudScore) < block {
			if ghostEnabled {
				out = append(out, SuspiciousIP{
					IP:         ip,
					Reason:     r.scorer.Name(),
					Score:      float64(fraudScore),
					CampaignID: featureRows[i].CampaignID,
					Action:     "ghost",
					Boost:      int32(fraudScore),
					TTLSeconds: 300, // 5 minutes TTL
				})
			}
		} else if uint8(fraudScore) >= block {
			out = append(out, SuspiciousIP{
				IP:         ip,
				Reason:     r.scorer.Name(),
				Score:      float64(fraudScore),
				CampaignID: featureRows[i].CampaignID,
				Action:     "blacklist",
				Boost:      int32(fraudScore),
				TTLSeconds: 3600, // 1 hour TTL for blacklist
			})
		}
	}

	return out, nil
}
