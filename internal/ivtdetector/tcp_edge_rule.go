package ivtdetector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"espx/internal/database"
	"espx/internal/edge/fingerprint"

	"github.com/redis/go-redis/v9"
)

type tcpEdgeCorrelationRule struct {
	q   *database.CHQuery
	rdb redis.Cmdable
	cfg AnalyzerConfig
}

func (r *tcpEdgeCorrelationRule) Name() string { return "tcp_edge_correlation" }

func (r *tcpEdgeCorrelationRule) Find(ctx context.Context) ([]SuspiciousIP, error) {
	if r == nil || r.q == nil || r.rdb == nil {
		return nil, nil
	}
	entries, err := fingerprint.ListRecent(ctx, r.rdb, 128)
	if err != nil {
		return nil, fmt.Errorf("list tcp fingerprints: %w", err)
	}
	if len(entries) == 0 {
		return nil, nil
	}

	ips := make([]string, 0, len(entries))
	seenIP := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if e.IP == "" {
			continue
		}
		if _, ok := seenIP[e.IP]; ok {
			continue
		}
		seenIP[e.IP] = struct{}{}
		ips = append(ips, e.IP)
	}
	if len(ips) == 0 {
		return nil, nil
	}

	windowSec := int64(r.cfg.Window / time.Second)
	if windowSec <= 0 {
		windowSec = 3600
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(ips)), ",")
	query := fmt.Sprintf(`
SELECT
    ip_address,
    any(user_agent) AS ua,
    any(tls_hash) AS ja3,
    any(toString(campaign_id)) AS campaign_id
FROM clicks
WHERE created_at >= now() - toIntervalSecond(?)
  AND ip_address IN (%s)
GROUP BY ip_address`, placeholders)

	args := make([]any, 0, 1+len(ips))
	args = append(args, windowSec)
	for _, ip := range ips {
		args = append(args, ip)
	}

	rows, err := r.q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("tcp edge correlation query: %w", err)
	}
	defer rows.Close()

	var out []SuspiciousIP
	for rows.Next() {
		var ip, ua, ja3, campaignID string
		if err := rows.Scan(&ip, &ua, &ja3, &campaignID); err != nil {
			return nil, fmt.Errorf("scan tcp edge row: %w", err)
		}
		if ip == "" || !IsTLSImpersonating(ua, ja3) {
			continue
		}
		out = append(out, SuspiciousIP{
			IP:         ip,
			CampaignID: campaignID,
			Reason:     "ivt_tcp_edge_correlation",
			Score:      70,
			Action:     "ghost",
			TTLSeconds: 3600,
		})
	}
	return out, rows.Err()
}
