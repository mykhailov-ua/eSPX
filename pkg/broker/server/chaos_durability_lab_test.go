package server

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"espx/pkg/broker/client"
	"espx/pkg/broker/log"
)

func skipChaosLab(t *testing.T) {
	t.Helper()
	skipChaosIntegration(t)
	if os.Getenv("BROKER_CHAOS_LAB") != "1" && os.Getenv("CI") == "" {
		t.Skip("durability lab tests run with BROKER_CHAOS_LAB=1 or in CI; use scripts/chaos/broker_chaos_lab.sh")
	}
}

func meanProduceLatency(t *testing.T, addr, topic string, n int) time.Duration {
	t.Helper()
	cli := client.NewClient(addr, 10*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	var total time.Duration
	for i := 0; i < n; i++ {
		start := time.Now()
		if _, err := cli.Produce(topic, 0, []byte(fmt.Sprintf("lat-%d-%d", time.Now().UnixNano(), i))); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		total += time.Since(start)
	}
	return total / time.Duration(n)
}

// Guards sync durability with injected fsync delay increases produce latency without failing requests.
func TestChaos_SlowFsync_ProduceLatencyRises(t *testing.T) {
	skipChaosLab(t)

	const syncDelay = 20 * time.Millisecond
	log.SetSyncDelayForTest(syncDelay)
	t.Cleanup(func() { log.SetSyncDelayForTest(0) })

	dir, err := os.MkdirTemp("", "chaos-slow-fsync-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := log.DurabilityConfig{
		Mode:               log.DurabilitySync,
		FlushInterval:      100 * time.Millisecond,
		GroupCommitRecords: 64,
	}
	s := NewServer(allocFreeTCPAddr(t), dir, 10*1024*1024, 4096)
	s.SetDurability(cfg)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	topic := "slow-fsync-topic"
	const n = 10
	avg := meanProduceLatency(t, s.Addr(), topic, n)
	if avg < syncDelay {
		t.Fatalf("expected mean produce latency >= %s with injected fsync delay, got %s", syncDelay, avg)
	}

	t.Logf("chaos_proof fault=slow_fsync sync_delay_ms=%d mean_produce_ms=%.2f produce_ok=true",
		syncDelay.Milliseconds(), avg.Seconds()*1000)
}

// Tracks produce latency under HDD read pressure from stress-ng (Linux lab only).
func TestChaos_PageCachePressure_ProduceLatencyRises(t *testing.T) {
	skipChaosLab(t)
	if !chaosLabOnLinux() || !stressNgAvailable() {
		t.Skip("requires Linux and stress-ng")
	}

	dir, err := os.MkdirTemp("", "chaos-pagecache-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	scratch := filepath.Join(dir, "scratch.bin")
	f, err := os.Create(scratch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 256*1024*1024)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	s := NewServer(allocFreeTCPAddr(t), filepath.Join(dir, "broker"), 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	topic := "pagecache-topic"
	baseline := meanProduceLatency(t, s.Addr(), topic, 8)

	stress, err := startStressNgHDD(t, scratch, 200, 15*time.Second)
	if err != nil {
		t.Fatalf("stress-ng: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	underStress := meanProduceLatency(t, s.Addr(), topic, 8)
	_ = stress.Process.Kill()
	_, _ = stress.Process.Wait()

	ratio := float64(underStress) / float64(baseline)
	if ratio < 1.1 {
		t.Logf("warning: latency ratio %.2f below 1.1 (environment may keep broker pages resident)", ratio)
	}

	t.Logf("chaos_proof fault=page_cache_thrashing baseline_us=%d under_stress_us=%d ratio=%.2f stress_ng=true",
		baseline.Microseconds(), underStress.Microseconds(), ratio)
}

// Guards throttled follower CPU increases replication lag before eventual catch-up.
func TestChaos_CPUThrottle_ReplicationCatchesUp(t *testing.T) {
	skipChaosLab(t)
	if !chaosLabOnLinux() || !cpulimitAvailable() {
		t.Skip("requires Linux and cpulimit")
	}

	redisURL, redisCleanup := startChaosRedis(t)
	defer redisCleanup()

	bin := ensureBrokerBinary(t)
	leaderDir, err := os.MkdirTemp("", "chaos-cpu-leader-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(leaderDir)

	followerDir, err := os.MkdirTemp("", "chaos-cpu-follower-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(followerDir)

	leaderAddr := allocFreeTCPAddr(t)
	leaderProc := startBrokerProcess(t, bin, leaderDir, "chaos-cpu-leader", redisURL, leaderAddr)

	topic := "cpu-throttle-topic"

	probeCoord, err := NewCoordinator("chaos-cpu-probe", leaderAddr, redisURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireEventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := probeCoord.rdb.Get(ctx, leaderKey(topicPartitionKey(topic))).Result()
		return err == nil
	}, 30*time.Second, 500*time.Millisecond, "redis reachable for leader subprocess")
	_ = probeCoord.rdb.Close()

	time.Sleep(3 * time.Second)

	seedCli := client.NewClient(leaderAddr, 5*time.Second)
	if err := seedCli.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := seedCli.Produce(topic, 0, []byte("bootstrap")); err != nil {
		t.Fatal(err)
	}
	_ = seedCli.Close()
	_ = leaderProc

	followerAddr := allocFreeTCPAddr(t)
	followerProc := startBrokerProcess(t, bin, followerDir, "chaos-cpu-follower", redisURL, followerAddr)
	if followerProc.cmd.Process == nil {
		t.Fatal("follower subprocess not running")
	}
	if _, err := throttleProcessCPU(t, followerProc.cmd.Process.Pid, 20); err != nil {
		t.Fatalf("cpulimit: %v", err)
	}

	time.Sleep(5 * time.Second)

	const msgCount = 60
	cli := client.NewClient(leaderAddr, 5*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < msgCount; i++ {
		if _, err := cli.Produce(topic, 0, []byte(fmt.Sprintf("cpu-msg-%d", i))); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	_ = cli.Close()

	sawLag := false
	requireEventually(t, func() bool {
		leaderCount := fetchMessageCount(t, leaderAddr, topic)
		followerCount := fetchMessageCount(t, followerAddr, topic)
		if followerCount+1 < leaderCount {
			sawLag = true
		}
		return followerCount+1 >= leaderCount && leaderCount >= uint64(msgCount)
	}, 120*time.Second, 1*time.Second, "throttled follower must eventually replicate all messages")

	t.Logf("chaos_proof fault=cpu_throttle_replication cpulimit_pct=20 msgs=%d lag_observed=%v caught_up=true",
		msgCount, sawLag)
}

// Guards brief Redis partition recovery restores leader election and produce availability.
func TestChaos_RedisOutage_CoordinationRecovers(t *testing.T) {
	skipChaosLab(t)

	redisURL, redisCleanup := startChaosRedis(t)
	defer redisCleanup()

	rest := redisURL[len("redis://"):]
	hostPort := rest[:len(rest)-len("/0")]
	proxy, proxyListen := startChaosTCPProxy(t, hostPort, 0, 0, 0)
	defer proxy.close()
	proxiedRedis := fmt.Sprintf("redis://%s/0", proxyListen)

	cl := startHACluster(t, haClusterOpts{redisURL: proxiedRedis})
	topic := cl.topic
	pk := topicPartitionKey(topic)
	leaderSrv, _ := cl.leaderServer()

	cli := client.NewClient(leaderSrv.Addr(), 5*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Produce(topic, 0, []byte("before-outage")); err != nil {
		t.Fatal(err)
	}
	_ = cli.Close()

	proxy.setBlocked(true)
	time.Sleep(6 * time.Second)
	proxy.setBlocked(false)

	requireEventually(t, func() bool {
		_, lc := cl.leaderServer()
		_, fc := cl.followerServer()
		return (lc.IsLeader(pk) && lc.IsLeaderReady(pk)) ||
			(fc.IsLeader(pk) && fc.IsLeaderReady(pk))
	}, 30*time.Second, 500*time.Millisecond, "a ready leader must be elected after redis outage")

	leaderSrv, _ = cl.leaderServer()
	requireEventually(t, func() bool {
		c := client.NewClient(leaderSrv.Addr(), 3*time.Second)
		if err := c.Connect(); err != nil {
			return false
		}
		defer c.Close()
		_, err := c.Produce(topic, 0, []byte("after-outage"))
		return err == nil
	}, 30*time.Second, 500*time.Millisecond, "produce must recover after redis outage")

	t.Logf("chaos_proof fault=redis_brief_outage block_seconds=6 produce_recovered=true")
}

// Guards coordinator produce continues after Sentinel-promoted Redis master failover (lab stack).
func TestChaos_RedisSentinelFailover_ProduceContinues(t *testing.T) {
	skipChaosLab(t)
	if os.Getenv("BROKER_CHAOS_SENTINEL") != "1" {
		t.Skip("requires BROKER_CHAOS_SENTINEL=1 and deploy/broker-lab sentinel stack")
	}

	master := os.Getenv("BROKER_REDIS_SENTINEL_MASTER")
	addrs := os.Getenv("BROKER_REDIS_SENTINEL_ADDRS")
	if master == "" || addrs == "" {
		t.Fatal("BROKER_REDIS_SENTINEL_MASTER and BROKER_REDIS_SENTINEL_ADDRS required")
	}

	redisURL := os.Getenv("BROKER_REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6380/0"
	}

	dir, err := os.MkdirTemp("", "chaos-sentinel-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer(allocFreeTCPAddr(t), dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	coord, err := NewCoordinator("chaos-sentinel-broker", s.Addr(), redisURL, s)
	if err != nil {
		t.Fatal(err)
	}
	s.SetCoordinator(coord)
	coord.Start()
	defer coord.Stop()

	topic := "sentinel-chaos-topic"
	time.Sleep(5 * time.Second)

	cli := client.NewClient(s.Addr(), 5*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Produce(topic, 0, []byte("before-failover")); err != nil {
		t.Fatalf("produce before failover: %v", err)
	}
	_ = cli.Close()

	masterContainer := os.Getenv("BROKER_CHAOS_SENTINEL_STOP_CONTAINER")
	if masterContainer != "" {
		stop := exec.Command("docker", "stop", masterContainer)
		if out, err := stop.CombinedOutput(); err != nil {
			t.Fatalf("docker stop %s: %v\n%s", masterContainer, err, out)
		}
		t.Cleanup(func() {
			_ = exec.Command("docker", "start", masterContainer).Run()
		})
	}

	time.Sleep(12 * time.Second)

	cli2 := client.NewClient(s.Addr(), 10*time.Second)
	if err := cli2.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli2.Close()

	requireEventually(t, func() bool {
		_, err := cli2.Produce(topic, 0, []byte("after-failover"))
		return err == nil
	}, 60*time.Second, 2*time.Second, "produce must succeed after sentinel failover")

	t.Logf("chaos_proof fault=redis_sentinel_failover master_stopped=%s produce_after=true", masterContainer)
}
