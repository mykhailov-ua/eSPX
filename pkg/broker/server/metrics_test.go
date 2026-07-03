package server

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"espx/pkg/broker/client"
	"espx/pkg/broker/protocol"
)

// TestMetricsEndpoint exposes broker counters after produce and fetch.
func TestMetricsEndpoint(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-metrics-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	s.SetHealthAddr("127.0.0.1:0")
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	time.Sleep(150 * time.Millisecond)

	topic := "metrics-topic"
	cli := client.NewClient(s.Addr(), 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if _, err := cli.Produce(topic, 0, []byte("metric-payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Fetch(topic, 0, 0, 4096); err != nil {
		t.Fatal(err)
	}

	metricsURL := "http://" + s.HealthAddr() + "/metrics"
	resp, err := http.Get(metricsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status: got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"ad_broker_produce_total",
		"ad_broker_fetch_total",
		"ad_broker_active_connections",
		"ad_broker_disk_writable",
		`topic="` + protocol.TopicPartitionID(topic, 0) + `"`,
		`status="ok"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics body missing %q", want)
		}
	}
}
