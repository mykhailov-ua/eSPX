package ivtdetector

import (
	"context"
	"fmt"
	"time"

	"espx/internal/database"
	"espx/internal/fraudscoring"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
)

type highCTRRule struct {
	analyzer *Analyzer
}

func (r *highCTRRule) Name() string { return "high_click_to_imp_ratio" }

func (r *highCTRRule) Find(ctx context.Context) ([]SuspiciousIP, error) {
	windowSec := int64(r.analyzer.cfg.Window / time.Second)
	if windowSec <= 0 {
		windowSec = 3600
	}
	return r.analyzer.findHighClickToImpRatio(ctx, windowSec)
}

type fingerprintRule struct {
	analyzer *Analyzer
}

func (r *fingerprintRule) Name() string { return "shared_fingerprint_cluster" }

func (r *fingerprintRule) Find(ctx context.Context) ([]SuspiciousIP, error) {
	windowSec := int64(r.analyzer.cfg.Window / time.Second)
	if windowSec <= 0 {
		windowSec = 3600
	}
	return r.analyzer.findSharedFingerprintClusters(ctx, windowSec)
}

type campaignCTRSpikeRule struct {
	q   *database.CHQuery
	cfg AnalyzerConfig
}

func (r *campaignCTRSpikeRule) Name() string { return "campaign_ctr_spike" }

func (r *campaignCTRSpikeRule) Find(ctx context.Context) ([]SuspiciousIP, error) {
	if r.q == nil {
		return nil, fmt.Errorf("campaign ctr rule: nil connection")
	}
	windowSec := int64(r.cfg.Window / time.Second)
	if windowSec <= 0 {
		windowSec = 3600
	}
	minClicks := r.cfg.MinClicks
	if minClicks == 0 {
		minClicks = 10
	}
	ratio := r.cfg.ClickToImpRatio
	if ratio <= 0 {
		ratio = 5.0
	}

	query := `
SELECT ip_address
FROM (
    SELECT
        campaign_id,
        ip_address,
        countIf(event_type = 'click') AS clicks,
        countIf(event_type = 'impression') AS imps
    FROM (
        SELECT campaign_id, ip_address, 'click' AS event_type FROM clicks
        WHERE created_at >= now() - toIntervalSecond(?)
          AND ip_address != ''
          AND campaign_id != toUUID('00000000-0000-0000-0000-000000000000')
        UNION ALL
        SELECT campaign_id, ip_address, 'impression' AS event_type FROM impressions
        WHERE created_at >= now() - toIntervalSecond(?)
          AND ip_address != ''
          AND campaign_id != toUUID('00000000-0000-0000-0000-000000000000')
    )
    GROUP BY campaign_id, ip_address
    HAVING clicks >= ?
       AND (imps < 1 OR toFloat64(clicks) / greatest(toFloat64(imps), 1.0) >= ?)
)
GROUP BY ip_address`

	rows, err := r.q.Query(ctx, query, windowSec, windowSec, minClicks, ratio)
	if err != nil {
		return nil, fmt.Errorf("campaign ctr spike query: %w", err)
	}
	defer rows.Close()

	var out []SuspiciousIP
	for rows.Next() {
		var ip string
		if err := rows.Scan(&ip); err != nil {
			return nil, fmt.Errorf("scan campaign ctr row: %w", err)
		}
		if ip == "" {
			continue
		}
		out = append(out, SuspiciousIP{
			IP:     ip,
			Reason: "ivt_campaign_ctr_spike",
			Score:  ratio,
		})
	}
	return out, rows.Err()
}

type datacenterASNRule struct {
	q   *database.CHQuery
	cfg AnalyzerConfig
	asn ASNClassifier
}

func (r *datacenterASNRule) Name() string { return "datacenter_asn" }

func (r *datacenterASNRule) Find(ctx context.Context) ([]SuspiciousIP, error) {
	if r.q == nil {
		return nil, fmt.Errorf("datacenter asn rule: nil connection")
	}
	windowSec := int64(r.cfg.Window / time.Second)
	if windowSec <= 0 {
		windowSec = 3600
	}
	minEvents := r.cfg.MinEventsPerIP
	if minEvents == 0 {
		minEvents = 5
	}

	query := `
SELECT ip_address, count() AS event_count
FROM (
    SELECT ip_address FROM clicks
    WHERE created_at >= now() - toIntervalSecond(?) AND ip_address != ''
    UNION ALL
    SELECT ip_address FROM impressions
    WHERE created_at >= now() - toIntervalSecond(?) AND ip_address != ''
)
GROUP BY ip_address
HAVING event_count >= ?`

	rows, err := r.q.Query(ctx, query, windowSec, windowSec, minEvents)
	if err != nil {
		return nil, fmt.Errorf("datacenter asn query: %w", err)
	}
	defer rows.Close()

	var out []SuspiciousIP
	for rows.Next() {
		var ip string
		var count uint64
		if err := rows.Scan(&ip, &count); err != nil {
			return nil, fmt.Errorf("scan datacenter asn row: %w", err)
		}
		if ip == "" || r.asn == nil || !r.asn.IsDatacenter(ip) {
			continue
		}
		out = append(out, SuspiciousIP{
			IP:     ip,
			Reason: "ivt_datacenter_asn",
			Score:  float64(count),
		})
	}
	return out, rows.Err()
}

// ASNClassifier enriches IPs with ASN metadata for datacenter detection.
type ASNClassifier interface {
	IsDatacenter(ip string) bool
}

// StaticASNClassifier flags known hosting ASN prefixes (tests and offline mode).
type StaticASNClassifier struct {
	DatacenterPrefixes []string
}

// IsDatacenter returns true when the IP matches a configured datacenter prefix.
func (c *StaticASNClassifier) IsDatacenter(ip string) bool {
	if c == nil {
		return false
	}
	for _, prefix := range c.DatacenterPrefixes {
		if prefix != "" && hasIPPrefix(ip, prefix) {
			return true
		}
	}
	return false
}

func hasIPPrefix(ip, prefix string) bool {
	if ip == prefix {
		return true
	}
	if len(prefix) > 0 && prefix[len(prefix)-1] == '.' {
		return len(ip) >= len(prefix) && ip[:len(prefix)] == prefix
	}
	return false
}

// NewAnalyzerRegistry wires default detection rules for production.
func NewAnalyzerRegistry(q *database.CHQuery, writeConn driver.Conn, pool *pgxpool.Pool, cfg AnalyzerConfig, asn ASNClassifier, scorer fraudscoring.Scorer, fraudScoringBatchSize int) *RuleRegistry {
	analyzer := NewAnalyzer(q, cfg)
	reg := NewRuleRegistry()
	reg.Register(&highCTRRule{analyzer: analyzer})
	reg.Register(&fingerprintRule{analyzer: analyzer})
	reg.Register(&campaignCTRSpikeRule{q: q, cfg: cfg})
	reg.Register(&intervalBotnetRule{q: q, cfg: cfg})
	if asn != nil {
		reg.Register(&datacenterASNRule{q: q, cfg: cfg, asn: asn})
	}
	if scorer != nil {
		reg.Register(NewFraudScoringRule(q, writeConn, pool, scorer, fraudScoringBatchSize))
	}
	return reg
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
