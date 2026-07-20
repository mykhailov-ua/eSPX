package ivtdetector

import (
	"context"
	"fmt"
	"time"

	"espx/internal/database"
)

const (
	defaultIntervalMinIntervals = 30
	defaultIntervalMaxVariance  = 0.005
	intervalBotReason           = "ivt_interval_bot"
)

// intervalBotnetRule flags /24 subnets with low inter-click arrival variance (timer bots).
type intervalBotnetRule struct {
	q   *database.CHQuery
	cfg AnalyzerConfig
}

func (r *intervalBotnetRule) Name() string { return "interval_bot" }

func (r *intervalBotnetRule) Find(ctx context.Context) ([]SuspiciousIP, error) {
	if r.q == nil {
		return nil, fmt.Errorf("interval bot rule: nil chquery")
	}

	windowSec := int64(r.cfg.Window / time.Second)
	if windowSec <= 0 {
		windowSec = 3600
	}
	minIntervals := r.cfg.IntervalMinIntervals
	if minIntervals == 0 {
		minIntervals = defaultIntervalMinIntervals
	}
	maxVariance := r.cfg.IntervalMaxVariance
	if maxVariance <= 0 {
		maxVariance = defaultIntervalMaxVariance
	}

	query := `
SELECT
    sample_ip,
    variance,
    n_intervals
FROM (
    SELECT
        subnet,
        any(ip_address) AS sample_ip,
        varPop(delta_t) AS variance,
        count() AS n_intervals
    FROM (
        SELECT
            ip_address,
            subnet,
            dateDiff(
                'millisecond',
                lagInFrame(created_at, 1, created_at) OVER (PARTITION BY subnet ORDER BY created_at),
                created_at
            ) / 1000.0 AS delta_t
        FROM (
            SELECT
                ip_address,
                created_at,
                IPv4NumToString(bitAnd(IPv4StringToNumOrDefault(ip_address), toUInt32(0xFFFFFF00))) AS subnet
            FROM clicks
            WHERE created_at >= now() - toIntervalSecond(?)
              AND ip_address != ''
              AND IPv4StringToNumOrDefault(ip_address) != 0
        )
    )
    WHERE delta_t > 0
    GROUP BY subnet
    HAVING n_intervals >= ? AND variance < ?
)
WHERE sample_ip != ''`

	rows, err := r.q.Query(ctx, query, windowSec, minIntervals, maxVariance)
	if err != nil {
		return nil, fmt.Errorf("interval bot query: %w", err)
	}
	defer rows.Close()

	var out []SuspiciousIP
	for rows.Next() {
		var ip string
		var variance float64
		var nIntervals uint64
		if err := rows.Scan(&ip, &variance, &nIntervals); err != nil {
			return nil, fmt.Errorf("scan interval bot row: %w", err)
		}
		if ip == "" {
			continue
		}
		out = append(out, SuspiciousIP{
			IP:     ip,
			Reason: intervalBotReason,
			Score:  variance,
		})
	}
	return out, rows.Err()
}

// popVariance computes population variance (ClickHouse varPop) for inter-arrival deltas.
func popVariance(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, v := range samples {
		sum += v
	}
	mean := sum / float64(len(samples))
	var acc float64
	for _, v := range samples {
		d := v - mean
		acc += d * d
	}
	return acc / float64(len(samples))
}

// isIntervalBot returns true when variance is below threshold with enough samples.
func isIntervalBot(deltas []float64, minIntervals uint64, maxVariance float64) bool {
	if uint64(len(deltas)) < minIntervals {
		return false
	}
	return popVariance(deltas) < maxVariance
}
