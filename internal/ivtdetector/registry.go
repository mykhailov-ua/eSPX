package ivtdetector

import (
	"context"
)

// SuspiciousFinder loads candidate IPs from ClickHouse or test doubles.
type SuspiciousFinder interface {
	FindSuspiciousIPs(ctx context.Context) ([]SuspiciousIP, error)
}

// Rule is a named detection plugin registered with the analyzer registry.
type Rule interface {
	Name() string
	Find(ctx context.Context) ([]SuspiciousIP, error)
}

// RuleRegistry runs registered detection rules and merges candidates.
type RuleRegistry struct {
	rules []Rule
}

// NewRuleRegistry constructs an empty finder registry.
func NewRuleRegistry() *RuleRegistry {
	return &RuleRegistry{}
}

// Register appends a detection rule to the registry.
func (r *RuleRegistry) Register(rule Rule) {
	if r == nil || rule == nil {
		return
	}
	r.rules = append(r.rules, rule)
}

// FindSuspiciousIPs executes all registered rules and deduplicates by IP.
func (r *RuleRegistry) FindSuspiciousIPs(ctx context.Context) ([]SuspiciousIP, error) {
	if r == nil || len(r.rules) == 0 {
		return nil, nil
	}
	var groups [][]SuspiciousIP
	for _, rule := range r.rules {
		found, err := rule.Find(ctx)
		if err != nil {
			return nil, err
		}
		if len(found) > 0 {
			groups = append(groups, found)
		}
		ivtCandidatesTotal.WithLabelValues(rule.Name()).Add(float64(len(found)))
	}
	return mergeSuspiciousIPs(groups...), nil
}
