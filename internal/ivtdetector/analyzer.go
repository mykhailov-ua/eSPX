package ivtdetector

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// SuspiciousIP is a candidate address flagged by a ClickHouse anomaly rule.
type SuspiciousIP struct {
	IP     string
	Reason string
	Score  float64
}

// AnalyzerConfig tunes ClickHouse window and detection thresholds.
type AnalyzerConfig struct {
	Window          time.Duration
	MinClicks       uint64
	MinImpressions  uint64
	ClickToImpRatio float64
	MinIPsPerUA     uint64
	MinEventsPerIP  uint64
}

// DefaultAnalyzerConfig returns production-oriented thresholds for IVT clustering.
func DefaultAnalyzerConfig() AnalyzerConfig {
	return AnalyzerConfig{
		Window:          time.Hour,
		MinClicks:       10,
		MinImpressions:  1,
		ClickToImpRatio: 5.0,
		MinIPsPerUA:     8,
		MinEventsPerIP:  5,
	}
}

// Analyzer runs cold-path ClickHouse queries for click-ratio and fingerprint-collision anomalies.
type Analyzer struct {
	conn driver.Conn
	cfg  AnalyzerConfig
}

// NewAnalyzer binds a ClickHouse connection and detection thresholds.
func NewAnalyzer(conn driver.Conn, cfg AnalyzerConfig) *Analyzer {
	return &Analyzer{conn: conn, cfg: cfg}
}

// FindSuspiciousIPs returns deduplicated candidates from all enabled detection rules.
func (analyzer *Analyzer) FindSuspiciousIPs(ctx context.Context) ([]SuspiciousIP, error) {
	if analyzer == nil || analyzer.conn == nil {
		return nil, fmt.Errorf("analyzer: nil connection")
	}

	windowSec := int64(analyzer.cfg.Window / time.Second)
	if windowSec <= 0 {
		windowSec = int64(time.Hour / time.Second)
	}

	highCTR, err := analyzer.findHighClickToImpRatio(ctx, windowSec)
	if err != nil {
		return nil, err
	}

	fingerprint, err := analyzer.findSharedFingerprintClusters(ctx, windowSec)
	if err != nil {
		return nil, err
	}

	return mergeSuspiciousIPs(highCTR, fingerprint), nil
}

func (analyzer *Analyzer) findHighClickToImpRatio(ctx context.Context, windowSec int64) ([]SuspiciousIP, error) {
	query := `
SELECT
    c.ip_address,
    c.click_count,
    coalesce(i.imp_count, toUInt64(0)) AS imp_count
FROM (
    SELECT ip_address, count() AS click_count
    FROM ad_event_processor.clicks
    WHERE created_at >= now() - toIntervalSecond(?)
      AND ip_address != ''
    GROUP BY ip_address
    HAVING click_count >= ?
) AS c
LEFT JOIN (
    SELECT ip_address, count() AS imp_count
    FROM ad_event_processor.impressions
    WHERE created_at >= now() - toIntervalSecond(?)
      AND ip_address != ''
    GROUP BY ip_address
) AS i ON c.ip_address = i.ip_address
WHERE c.click_count >= ?
  AND (
    imp_count < ?
    OR (toFloat64(c.click_count) / greatest(toFloat64(imp_count), 1.0)) >= ?
  )`

	rows, err := analyzer.conn.Query(
		ctx,
		query,
		windowSec,
		analyzer.cfg.MinClicks,
		windowSec,
		analyzer.cfg.MinClicks,
		analyzer.cfg.MinImpressions,
		analyzer.cfg.ClickToImpRatio,
	)
	if err != nil {
		return nil, fmt.Errorf("high click-to-imp query: %w", err)
	}
	defer rows.Close()

	var out []SuspiciousIP
	for rows.Next() {
		var ip string
		var clickCount, impCount uint64
		if err := rows.Scan(&ip, &clickCount, &impCount); err != nil {
			return nil, fmt.Errorf("scan high click-to-imp row: %w", err)
		}
		if ip == "" {
			continue
		}
		ratio := float64(clickCount)
		if impCount > 0 {
			ratio /= float64(impCount)
		}
		out = append(out, SuspiciousIP{
			IP:     ip,
			Reason: "ivt_high_click_to_imp_ratio",
			Score:  ratio,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate high click-to-imp rows: %w", err)
	}
	return out, nil
}

func (analyzer *Analyzer) findSharedFingerprintClusters(ctx context.Context, windowSec int64) ([]SuspiciousIP, error) {
	query := `
SELECT ip_address
FROM (
    SELECT
        user_agent,
        groupUniqArray(ip_address) AS ips,
        uniqExact(ip_address) AS ip_count
    FROM (
        SELECT ip_address, user_agent
        FROM ad_event_processor.impressions
        WHERE created_at >= now() - toIntervalSecond(?)
          AND ip_address != ''
          AND user_agent != ''
        UNION ALL
        SELECT ip_address, user_agent
        FROM ad_event_processor.clicks
        WHERE created_at >= now() - toIntervalSecond(?)
          AND ip_address != ''
          AND user_agent != ''
    )
    GROUP BY user_agent
    HAVING ip_count >= ?
)
ARRAY JOIN ips AS ip_address
GROUP BY ip_address
HAVING count() >= 1`

	rows, err := analyzer.conn.Query(
		ctx,
		query,
		windowSec,
		windowSec,
		analyzer.cfg.MinIPsPerUA,
	)
	if err != nil {
		return nil, fmt.Errorf("shared fingerprint query: %w", err)
	}
	defer rows.Close()

	var out []SuspiciousIP
	for rows.Next() {
		var ip string
		if err := rows.Scan(&ip); err != nil {
			return nil, fmt.Errorf("scan shared fingerprint row: %w", err)
		}
		if ip == "" {
			continue
		}
		out = append(out, SuspiciousIP{
			IP:     ip,
			Reason: "ivt_shared_fingerprint_cluster",
			Score:  float64(analyzer.cfg.MinIPsPerUA),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate shared fingerprint rows: %w", err)
	}
	return out, nil
}

func mergeSuspiciousIPs(groups ...[]SuspiciousIP) []SuspiciousIP {
	seen := make(map[string]SuspiciousIP)
	for _, group := range groups {
		for _, candidate := range group {
			existing, ok := seen[candidate.IP]
			if !ok || candidate.Score > existing.Score {
				seen[candidate.IP] = candidate
			}
		}
	}
	out := make([]SuspiciousIP, 0, len(seen))
	for _, candidate := range seen {
		out = append(out, candidate)
	}
	return out
}
