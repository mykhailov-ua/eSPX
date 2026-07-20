package installer

import (
	"encoding/json"
	"fmt"
)

// CheckStatus is the outcome of a single preflight probe.
type CheckStatus string

const (
	StatusPass CheckStatus = "PASS"
	StatusFail CheckStatus = "FAIL"
	StatusWarn CheckStatus = "WARN"
)

// PreflightCheck is one PF-* probe result emitted by preflight.
type PreflightCheck struct {
	ID          string      `json:"id"`
	Description string      `json:"description"`
	Status      CheckStatus `json:"status"`
	Message     string      `json:"message,omitempty"`
}

// PreflightResults aggregates all PF-* checks for human or JSON output.
type PreflightResults struct {
	Checks []PreflightCheck `json:"checks"`
	Passed bool             `json:"passed"`
}

func RunPreflight(strict bool, asJSON bool) (*PreflightResults, error) {
	checks := getPreflightChecks()
	results := &PreflightResults{
		Checks: make([]PreflightCheck, 0, len(checks)),
		Passed: true,
	}

	for _, check := range checks {
		res := check()
		results.Checks = append(results.Checks, res)
		if res.Status == StatusFail {
			results.Passed = false
		}
		if strict && res.Status == StatusWarn {
			results.Passed = false
		}
	}

	if asJSON {
		data, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			return results, err
		}
		fmt.Println(string(data))
		return results, nil
	}

	for _, res := range results.Checks {
		fmt.Printf("[%s] %s: %s %s\n", res.Status, res.ID, res.Description, res.Message)
	}

	if strict && !results.Passed {
		return results, fmt.Errorf("preflight failed in strict mode")
	}

	return results, nil
}

type checkFunc func() PreflightCheck
