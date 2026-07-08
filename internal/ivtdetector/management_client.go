package ivtdetector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// BlacklistBlocker enqueues fraud blacklist entries via management HTTP or in-process service.
type BlacklistBlocker interface {
	BlockIP(ctx context.Context, ip string) error
}

const blacklistSourceFraud = "fraud"

// ManagementClient posts blacklist entries to the management admin API.
type ManagementClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewManagementClient dials the management HTTP surface for blacklist writes.
func NewManagementClient(baseURL, apiKey string, timeout time.Duration) *ManagementClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &ManagementClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// BlockIP enqueues a fraud blacklist entry via POST /admin/blacklist.
func (client *ManagementClient) BlockIP(ctx context.Context, ip string) error {
	if client == nil {
		return fmt.Errorf("management client: nil receiver")
	}
	if ip == "" {
		return ErrInvalidIP
	}

	body, err := json.Marshal(map[string]string{
		"ip":     ip,
		"source": blacklistSourceFraud,
	})
	if err != nil {
		return fmt.Errorf("marshal blacklist request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, client.baseURL+"/admin/blacklist", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build blacklist request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-API-Key", client.apiKey)

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrManagementUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusCreated {
		return nil
	}

	payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if readErr != nil {
		return fmt.Errorf("%w: status=%d read body: %v", ErrManagementUnavailable, resp.StatusCode, readErr)
	}
	return fmt.Errorf("%w: status=%d body=%s", ErrManagementUnavailable, resp.StatusCode, strings.TrimSpace(string(payload)))
}
