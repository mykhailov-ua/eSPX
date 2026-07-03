package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"espx/pkg/broker/client"
	"espx/pkg/broker/protocol"

	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// skipChaosIntegration skips broker chaos tests when -short is set.
func skipChaosIntegration(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping chaos integration test in short mode")
	}
}

// topicPartitionKey maps a logical topic name to the broker storage/coordination key for partition 0.
func topicPartitionKey(topic string) string {
	return protocol.TopicPartitionID(topic, 0)
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func allocFreeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("alloc listen addr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func startChaosRedis(t *testing.T) (redisURL string, cleanup func()) {
	t.Helper()
	skipChaosIntegration(t)

	ctx := context.Background()
	c, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}
	ep, err := c.Endpoint(ctx, "")
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("redis endpoint: %v", err)
	}
	return fmt.Sprintf("redis://%s/0", ep), func() {
		_ = c.Terminate(ctx)
	}
}

// chaosTCPProxy forwards TCP with optional latency, jitter, and connection loss.
type chaosTCPProxy struct {
	listen  net.Listener
	target  string
	latency time.Duration
	jitter  time.Duration
	lossPct int

	wg      sync.WaitGroup
	closeMu sync.Mutex
	closed  bool
	blocked bool
	conns   []net.Conn
}

func startChaosTCPProxy(t *testing.T, target string, latency, jitter time.Duration, lossPct int) (*chaosTCPProxy, string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &chaosTCPProxy{
		listen:  ln,
		target:  target,
		latency: latency,
		jitter:  jitter,
		lossPct: lossPct,
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			p.closeMu.Lock()
			blocked := p.blocked
			p.closeMu.Unlock()
			if blocked {
				_ = client.Close()
				continue
			}
			if lossPct > 0 && rand.IntN(100) < lossPct {
				_ = client.Close()
				continue
			}
			p.wg.Add(1)
			go func(c net.Conn) {
				defer p.wg.Done()
				p.serve(c)
			}(client)
		}
	}()
	return p, ln.Addr().String()
}

func (p *chaosTCPProxy) serve(client net.Conn) {
	defer client.Close()

	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		return
	}
	p.trackConn(upstream)
	defer upstream.Close()

	var wg sync.WaitGroup
	pipe := func(dst, src net.Conn) {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if p.lossPct > 0 && rand.IntN(100) < p.lossPct {
					return
				}
				if d := p.latency; d > 0 {
					j := time.Duration(0)
					if p.jitter > 0 {
						j = time.Duration(rand.Int64N(int64(p.jitter)))
					}
					time.Sleep(d + j)
				}
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go pipe(upstream, client)
	go pipe(client, upstream)
	wg.Wait()
}

func (p *chaosTCPProxy) trackConn(c net.Conn) {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	if p.closed {
		return
	}
	p.conns = append(p.conns, c)
}

func (p *chaosTCPProxy) close() {
	p.closeMu.Lock()
	p.closed = true
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
	p.closeMu.Unlock()

	_ = p.listen.Close()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
}

// setBlocked drops new and existing proxy connections when true (Redis outage simulation).
func (p *chaosTCPProxy) setBlocked(block bool) {
	p.closeMu.Lock()
	p.blocked = block
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
	p.closeMu.Unlock()
}

func waitTCPReady(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("tcp %s not ready within %s", addr, timeout)
}

type haCluster struct {
	redisURL      string
	topic         string
	leader        *Server
	follower      *Server
	leaderCoord   *Coordinator
	followerCoord *Coordinator
	leaderID      string
	followerID    string
}

type haClusterOpts struct {
	leaderHeartbeatAddr string
	redisURL            string
	leaderRedisURL      string
	followerRedisURL    string
	coordCfg            CoordConfig
}

func fastCoordConfig() CoordConfig {
	return CoordConfig{
		LeaseTTL:           5 * time.Second,
		Interval:           1 * time.Second,
		RenewFailThreshold: 2,
		DebounceWindow:     1 * time.Second,
	}
}

func coordRedisURL(opts haClusterOpts, role string) string {
	switch role {
	case "leader":
		if opts.leaderRedisURL != "" {
			return opts.leaderRedisURL
		}
	case "follower":
		if opts.followerRedisURL != "" {
			return opts.followerRedisURL
		}
	}
	return opts.redisURL
}

// startRedisBehindProxy returns direct Redis URL and a proxied URL for partition experiments.
func startRedisBehindProxy(t *testing.T) (directURL, proxyURL string, proxy *chaosTCPProxy) {
	t.Helper()
	directURL, cleanup := startChaosRedis(t)
	t.Cleanup(cleanup)

	rest := strings.TrimPrefix(directURL, "redis://")
	hostPort, _, _ := strings.Cut(rest, "/")
	proxy, listenAddr := startChaosTCPProxy(t, hostPort, 0, 0, 0)
	t.Cleanup(func() { proxy.close() })
	proxyURL = fmt.Sprintf("redis://%s/0", listenAddr)
	return directURL, proxyURL, proxy
}

func startHACluster(t *testing.T, opts haClusterOpts) *haCluster {
	t.Helper()

	redisURL := opts.redisURL
	var redisCleanup func()
	if redisURL == "" {
		var cleanup func()
		redisURL, cleanup = startChaosRedis(t)
		redisCleanup = cleanup
		t.Cleanup(redisCleanup)
		opts.redisURL = redisURL
	}

	dir1, err := os.MkdirTemp("", "chaos-ha-leader-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir1) })

	dir2, err := os.MkdirTemp("", "chaos-ha-follower-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir2) })

	leader := NewServer(allocFreeTCPAddr(t), dir1, 10*1024*1024, 4096)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { leader.Stop() })

	follower := NewServer(allocFreeTCPAddr(t), dir2, 10*1024*1024, 4096)
	if err := follower.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { follower.Stop() })

	heartbeatAddr := opts.leaderHeartbeatAddr
	if heartbeatAddr == "" {
		heartbeatAddr = leader.Addr()
	}

	coordCfg := opts.coordCfg
	if coordCfg.LeaseTTL == 0 {
		coordCfg = DefaultCoordConfig()
	}

	leaderCoord, err := NewCoordinatorWithConfig("chaos-leader", heartbeatAddr, coordRedisURL(opts, "leader"), leader, coordCfg)
	if err != nil {
		t.Fatal(err)
	}
	leader.SetCoordinator(leaderCoord)
	leaderCoord.Start()
	t.Cleanup(func() { leaderCoord.Stop() })

	followerCoord, err := NewCoordinatorWithConfig("chaos-follower", follower.Addr(), coordRedisURL(opts, "follower"), follower, coordCfg)
	if err != nil {
		t.Fatal(err)
	}
	follower.SetCoordinator(followerCoord)
	followerCoord.Start()
	t.Cleanup(func() { followerCoord.Stop() })

	cl := &haCluster{
		redisURL:      redisURL,
		topic:         "chaos-ha-topic",
		leader:        leader,
		follower:      follower,
		leaderCoord:   leaderCoord,
		followerCoord: followerCoord,
		leaderID:      "chaos-leader",
		followerID:    "chaos-follower",
	}

	waitForTopicLeader(t, cl, 15*time.Second)
	return cl
}

// startHAPartitionedCluster boots leader-through-proxy before follower so chaos-leader holds leadership.
func startHAPartitionedCluster(t *testing.T) (*haCluster, *chaosTCPProxy) {
	t.Helper()

	directURL, proxyURL, proxy := startRedisBehindProxy(t)
	cfg := fastCoordConfig()
	topic := "chaos-ha-topic"
	pk := topicPartitionKey(topic)

	dir1, err := os.MkdirTemp("", "chaos-part-leader-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir1) })

	leader := NewServer(allocFreeTCPAddr(t), dir1, 10*1024*1024, 4096)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { leader.Stop() })

	leaderCoord, err := NewCoordinatorWithConfig("chaos-leader", leader.Addr(), proxyURL, leader, cfg)
	if err != nil {
		t.Fatal(err)
	}
	leader.SetCoordinator(leaderCoord)
	leaderCoord.Start()
	t.Cleanup(func() { leaderCoord.Stop() })

	if _, err := leader.getOrCreatePartition(pk); err != nil {
		t.Fatal(err)
	}

	requireEventually(t, func() bool {
		return leaderCoord.IsLeader(pk)
	}, 20*time.Second, 500*time.Millisecond, "chaos-leader must win election before follower starts")

	dir2, err := os.MkdirTemp("", "chaos-part-follower-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir2) })

	follower := NewServer(allocFreeTCPAddr(t), dir2, 10*1024*1024, 4096)
	if err := follower.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { follower.Stop() })

	followerCoord, err := NewCoordinatorWithConfig("chaos-follower", follower.Addr(), directURL, follower, cfg)
	if err != nil {
		t.Fatal(err)
	}
	follower.SetCoordinator(followerCoord)
	followerCoord.Start()
	t.Cleanup(func() { followerCoord.Stop() })

	if _, err := follower.getOrCreatePartition(pk); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	cl := &haCluster{
		redisURL:      directURL,
		topic:         topic,
		leader:        leader,
		follower:      follower,
		leaderCoord:   leaderCoord,
		followerCoord: followerCoord,
		leaderID:      "chaos-leader",
		followerID:    "chaos-follower",
	}
	if !leaderCoord.IsLeader(pk) {
		t.Fatal("partitioned cluster requires chaos-leader as initial leader")
	}
	return cl, proxy
}

// startHAClusterOrdered boots chaos-leader before chaos-follower so leadership is deterministic.
func startHAClusterOrdered(t *testing.T) *haCluster {
	t.Helper()

	redisURL, cleanup := startChaosRedis(t)
	t.Cleanup(cleanup)

	cfg := fastCoordConfig()
	topic := "chaos-ha-topic"
	pk := topicPartitionKey(topic)

	dir1, err := os.MkdirTemp("", "chaos-ord-leader-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir1) })

	leader := NewServer(allocFreeTCPAddr(t), dir1, 10*1024*1024, 4096)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { leader.Stop() })

	leaderCoord, err := NewCoordinatorWithConfig("chaos-leader", leader.Addr(), redisURL, leader, cfg)
	if err != nil {
		t.Fatal(err)
	}
	leader.SetCoordinator(leaderCoord)
	leaderCoord.Start()
	t.Cleanup(func() { leaderCoord.Stop() })

	if _, err := leader.getOrCreatePartition(pk); err != nil {
		t.Fatal(err)
	}

	requireEventually(t, func() bool {
		return leaderCoord.IsLeader(pk)
	}, 20*time.Second, 500*time.Millisecond, "chaos-leader must win election before follower starts")

	dir2, err := os.MkdirTemp("", "chaos-ord-follower-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir2) })

	follower := NewServer(allocFreeTCPAddr(t), dir2, 10*1024*1024, 4096)
	if err := follower.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { follower.Stop() })

	followerCoord, err := NewCoordinatorWithConfig("chaos-follower", follower.Addr(), redisURL, follower, cfg)
	if err != nil {
		t.Fatal(err)
	}
	follower.SetCoordinator(followerCoord)
	followerCoord.Start()
	t.Cleanup(func() { followerCoord.Stop() })

	if _, err := follower.getOrCreatePartition(pk); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	return &haCluster{
		redisURL:      redisURL,
		topic:         topic,
		leader:        leader,
		follower:      follower,
		leaderCoord:   leaderCoord,
		followerCoord: followerCoord,
		leaderID:      "chaos-leader",
		followerID:    "chaos-follower",
	}
}

func waitForTopicLeader(t *testing.T, cl *haCluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pk := topicPartitionKey(cl.topic)
		if _, err := cl.leader.getOrCreatePartition(pk); err == nil {
			_, _ = cl.follower.getOrCreatePartition(pk)
		}
		if cl.leaderCoord.IsLeader(pk) || cl.followerCoord.IsLeader(pk) {
			time.Sleep(2 * time.Second)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("no broker elected leader for topic")
}

func (cl *haCluster) leaderServer() (*Server, *Coordinator) {
	pk := topicPartitionKey(cl.topic)
	if cl.leaderCoord.IsLeader(pk) {
		return cl.leader, cl.leaderCoord
	}
	return cl.follower, cl.followerCoord
}

func (cl *haCluster) followerServer() (*Server, *Coordinator) {
	pk := topicPartitionKey(cl.topic)
	if cl.leaderCoord.IsLeader(pk) {
		return cl.follower, cl.followerCoord
	}
	return cl.leader, cl.leaderCoord
}

func partitionOffset(t *testing.T, s *Server, topic string) uint64 {
	t.Helper()
	pl, err := s.getOrCreatePartition(topicPartitionKey(topic))
	if err != nil {
		t.Fatalf("partition: %v", err)
	}
	return pl.NextOffset()
}

func produceMessages(t *testing.T, addr, topic string, count int) {
	t.Helper()
	cli := client.NewClient(addr, 5*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	for i := 0; i < count; i++ {
		payload := []byte(fmt.Sprintf("chaos-msg-%d", i))
		off, err := cli.Produce(topic, 0, payload)
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		if off != uint64(i) {
			t.Fatalf("produce %d: offset %d want %d", i, off, i)
		}
	}
}

func verifyPartitionPayloads(t *testing.T, s *Server, topic string, expectedCount uint64) {
	t.Helper()
	cli := client.NewClient(s.Addr(), 5*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	iter, err := cli.Fetch(topic, 0, 0, 16*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	var i uint64
	for iter.Next() {
		if i >= expectedCount {
			break
		}
		want := fmt.Sprintf("chaos-msg-%d", i)
		if string(iter.Payload) != want {
			t.Fatalf("payload %d: got %q want %q", i, iter.Payload, want)
		}
		if iter.Offset != i {
			t.Fatalf("offset %d: got %d", i, iter.Offset)
		}
		i++
	}
	if i != expectedCount {
		t.Fatalf("expected %d messages, got %d", expectedCount, i)
	}
}

func readRedisEpoch(t *testing.T, coord *Coordinator, topic string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	val, err := coord.rdb.Get(ctx, leaderEpochKey(topicPartitionKey(topic))).Result()
	if err != nil {
		return 0
	}
	var epoch uint64
	_, _ = fmt.Sscanf(val, "%d", &epoch)
	return epoch
}

type brokerProcess struct {
	cmd  *exec.Cmd
	addr string
	dir  string
}

func ensureBrokerBinary(t *testing.T) string {
	t.Helper()
	root := moduleRoot(t)
	bin := filepath.Join(os.TempDir(), "espx-broker-lab-test")
	if st, err := os.Stat(bin); err == nil && st.Mode().IsRegular() {
		return bin
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/broker")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build broker: %v\n%s", err, out)
	}
	return bin
}

func startBrokerProcess(t *testing.T, bin, dataDir, nodeID, redisURL, addr string, extraArgs ...string) *brokerProcess {
	t.Helper()

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}

	args := []string{
		"-addr", addr,
		"-health-addr", allocFreeTCPAddr(t),
		"-data-dir", dataDir,
		"-node-id", nodeID,
		"-redis-url", redisURL,
	}
	args = append(args, extraArgs...)
	cmd := exec.Command(bin, args...)
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker: %v\nstderr: %s", err, stderr.String())
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	if err := waitTCPReady(addr, 30*time.Second); err != nil {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
		t.Fatalf("broker not ready on %s: %v\nstderr: %s", addr, err, stderr.String())
	}
	// gnet boot + coordinator attach need a short settle window.
	time.Sleep(300 * time.Millisecond)
	return &brokerProcess{cmd: cmd, addr: addr, dir: dataDir}
}

func (p *brokerProcess) kill9(t *testing.T) {
	t.Helper()
	if p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
	p.cmd.Process = nil
}

func fetchMessageCount(t *testing.T, addr, topic string) uint64 {
	t.Helper()
	cli := client.NewClient(addr, 5*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	iter, err := cli.Fetch(topic, 0, 0, 32*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for iter.Next() {
		n++
	}
	return n
}
