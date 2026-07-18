package licensing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// LicenseClient communicates with the vendor license-server.
type LicenseClient struct {
	serverURL  string
	licenseKey string
	httpClient *http.Client

	mu           sync.Mutex
	failures     int
	lastFailedAt time.Time
	tripped      bool
}

type HeartbeatPayload struct {
	LicenseKey    string `json:"license_key"`
	DeploymentID  string `json:"deployment_id"`
	Fingerprint   string `json:"fingerprint"`
	Version       string `json:"version"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}

type ActivatePayload struct {
	LicenseKey   string `json:"license_key"`
	DeploymentID string `json:"deployment_id"`
	Fingerprint  string `json:"fingerprint"`
}

func NewLicenseClient(serverURL, licenseKey string, timeout time.Duration) *LicenseClient {
	return &LicenseClient{
		serverURL:  serverURL,
		licenseKey: licenseKey,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// IsTripped checks if the circuit breaker is currently open.
func (c *LicenseClient) IsTripped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tripped {
		if time.Since(c.lastFailedAt) >= 1*time.Minute {
			// Half-open probe
			return false
		}
		return true
	}
	return false
}

func (c *LicenseClient) recordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.tripped = false
}

func (c *LicenseClient) recordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures++
	c.lastFailedAt = time.Now()
	if c.failures >= 3 {
		c.tripped = true
	}
}

func (c *LicenseClient) Activate(ctx context.Context, deploymentID, fingerprint string) (string, error) {
	if c.IsTripped() {
		return "", errors.New("license client circuit breaker tripped")
	}

	url := fmt.Sprintf("%s/v1/activate", c.serverURL)
	payload := ActivatePayload{
		LicenseKey:   c.licenseKey,
		DeploymentID: deploymentID,
		Fingerprint:  fingerprint,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.recordFailure()
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.recordFailure()
		return "", fmt.Errorf("activation failed: status %d", resp.StatusCode)
	}

	var res struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}

	c.recordSuccess()
	return res.Token, nil
}

func (c *LicenseClient) Heartbeat(ctx context.Context, deploymentID, fingerprint string, uptime int64) (string, bool, error) {
	if c.IsTripped() {
		return "", false, errors.New("license client circuit breaker tripped")
	}

	url := fmt.Sprintf("%s/v1/heartbeat", c.serverURL)
	payload := HeartbeatPayload{
		LicenseKey:    c.licenseKey,
		DeploymentID:  deploymentID,
		Fingerprint:   fingerprint,
		Version:       "1.0.0",
		UptimeSeconds: uptime,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", false, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.recordFailure()
		return "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		c.recordSuccess()
		return "", true, nil
	}

	if resp.StatusCode != http.StatusOK {
		c.recordFailure()
		return "", false, fmt.Errorf("heartbeat failed: status %d", resp.StatusCode)
	}

	var res struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", false, err
	}

	c.recordSuccess()
	return res.Token, false, nil
}
