package server

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func scrapeBrokerMetrics(t *testing.T, healthAddr string) string {
	t.Helper()
	if healthAddr == "" {
		t.Fatal("health addr is empty")
	}
	var lastErr error
	var body string
	for i := 0; i < 10; i++ {
		resp, err := http.Get("http://" + healthAddr + "/metrics")
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = readErr
			time.Sleep(100 * time.Millisecond)
			continue
		}
		body = string(data)
		break
	}
	if body == "" {
		t.Fatalf("scrape metrics from %s: %v", healthAddr, lastErr)
	}
	return body
}

func metricsHasGaugeValue(t *testing.T, body, name, labelFragment string, wantValue string) {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, name) {
			continue
		}
		if labelFragment != "" && !strings.Contains(line, labelFragment) {
			continue
		}
		if strings.HasSuffix(line, " "+wantValue) {
			return
		}
	}
	t.Fatalf("metrics missing gauge %s{%s} %s", name, labelFragment, wantValue)
}

func metricsHasCounterIncrement(t *testing.T, body, name, labelFragment string) {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, name) {
			continue
		}
		if labelFragment != "" && !strings.Contains(line, labelFragment) {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		if parts[len(parts)-1] != "0" {
			return
		}
	}
	t.Fatalf("metrics missing counter increment %s{%s}", name, labelFragment)
}
