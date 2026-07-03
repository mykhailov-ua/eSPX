package server

import (
	"net/http"
	"os"
	"testing"
	"time"
)

// Guards readonly data directory surfaces unhealthy healthz without panicking.
func TestChaos_ReadonlyDataDir_HealthzUnavailable(t *testing.T) {
	dir, err := os.MkdirTemp("", "chaos-disk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.Chmod(dir, 0o755)
		os.RemoveAll(dir)
	}()

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	s.SetHealthAddr("127.0.0.1:0")
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	time.Sleep(150 * time.Millisecond)
	healthURL := "http://" + s.HealthAddr() + "/healthz"

	if code := httpGet(t, healthURL); code != http.StatusOK {
		t.Fatalf("baseline healthz: expected 200, got %d", code)
	}

	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}

	requireEventually(t, func() bool {
		return !s.probeDisk()
	}, 8*time.Second, 200*time.Millisecond, "probeDisk must fail on readonly data dir")

	requireEventually(t, func() bool {
		return httpGet(t, healthURL) == http.StatusServiceUnavailable
	}, 8*time.Second, 500*time.Millisecond, "healthz must flip to 503 after disk worker observes readonly dir")

	if code := httpGet(t, healthURL); code != http.StatusServiceUnavailable {
		t.Fatalf("after chmod 0000: expected healthz 503, got %d", code)
	}

	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	requireEventually(t, func() bool {
		return s.probeDisk()
	}, 8*time.Second, 200*time.Millisecond, "probeDisk must recover after chmod restore")

	requireEventually(t, func() bool {
		return httpGet(t, healthURL) == http.StatusOK
	}, 8*time.Second, 500*time.Millisecond, "healthz must recover after disk writable again")

	t.Log("chaos_proof fault=chmod_data_dir_0000 probe_failed=true healthz_503=true recovered=true")
}

func requireEventually(t *testing.T, fn func() bool, timeout, interval time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatal(msg)
}
