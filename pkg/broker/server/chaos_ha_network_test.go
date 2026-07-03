package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/pkg/broker/client"
	"espx/pkg/broker/log"
)

// Guards replication catches up after leader TCP path latency injection.
func TestChaos_Replication_HighLatency(t *testing.T) {
	skipChaosIntegration(t)

	redisURL, redisCleanup := startChaosRedis(t)
	defer redisCleanup()

	dir1, err := os.MkdirTemp("", "chaos-lat-leader-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir1)

	dir2, err := os.MkdirTemp("", "chaos-lat-follower-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir2)

	leader := NewServer(allocFreeTCPAddr(t), dir1, 10*1024*1024, 4096)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	defer leader.Stop()

	proxy, proxyAddr := startChaosTCPProxy(t, leader.Addr(), 80*time.Millisecond, 20*time.Millisecond, 0)
	defer proxy.close()

	follower := NewServer(allocFreeTCPAddr(t), dir2, 10*1024*1024, 4096)
	if err := follower.Start(); err != nil {
		t.Fatal(err)
	}
	defer follower.Stop()

	leaderCoord, err := NewCoordinator("chaos-lat-leader", proxyAddr, redisURL, leader)
	if err != nil {
		t.Fatal(err)
	}
	leader.SetCoordinator(leaderCoord)
	leaderCoord.Start()
	defer leaderCoord.Stop()

	followerCoord, err := NewCoordinator("chaos-lat-follower", follower.Addr(), redisURL, follower)
	if err != nil {
		t.Fatal(err)
	}
	follower.SetCoordinator(followerCoord)
	followerCoord.Start()
	defer followerCoord.Stop()

	topic := "latency-chaos-topic"
	waitForTopicLeader(t, &haCluster{
		leader: leader, follower: follower,
		leaderCoord: leaderCoord, followerCoord: followerCoord,
		topic: topic,
	}, 20*time.Second)

	leaderSrv, _ := (&haCluster{
		leader: leader, follower: follower,
		leaderCoord: leaderCoord, followerCoord: followerCoord,
		topic: topic,
	}).leaderServer()
	followerSrv, _ := (&haCluster{
		leader: leader, follower: follower,
		leaderCoord: leaderCoord, followerCoord: followerCoord,
		topic: topic,
	}).followerServer()

	const msgCount = 80
	produceMessages(t, leaderSrv.Addr(), topic, msgCount)

	sawLag := false
	requireEventually(t, func() bool {
		lo := partitionOffset(t, leaderSrv, topic)
		fo := partitionOffset(t, followerSrv, topic)
		if fo < lo {
			sawLag = true
		}
		return fo == lo
	}, 90*time.Second, 500*time.Millisecond, "follower must catch up to leader under latency")

	if !sawLag {
		t.Log("warning: replication lag was not observed before catch-up (timing may vary)")
	}

	verifyPartitionPayloads(t, followerSrv, topic, msgCount)
	metricsBody := scrapeBrokerMetrics(t, follower.HealthAddr())
	pk := topicPartitionKey(topic)
	metricsHasGaugeValue(t, metricsBody, "ad_broker_replication_lag_messages", `topic="`+pk+`"`, "0")
	t.Logf("chaos_proof fault=replication_high_latency lag_observed=%v msgs=%d caught_up=true", sawLag, msgCount)
}

// Guards replication survives lossy leader TCP paths.
func TestChaos_Replication_PacketLoss(t *testing.T) {
	skipChaosIntegration(t)

	redisURL, redisCleanup := startChaosRedis(t)
	defer redisCleanup()

	dir1, err := os.MkdirTemp("", "chaos-loss-leader-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir1)

	dir2, err := os.MkdirTemp("", "chaos-loss-follower-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir2)

	leader := NewServer(allocFreeTCPAddr(t), dir1, 10*1024*1024, 4096)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	defer leader.Stop()

	proxy, proxyAddr := startChaosTCPProxy(t, leader.Addr(), 0, 0, 5)
	defer proxy.close()

	follower := NewServer(allocFreeTCPAddr(t), dir2, 10*1024*1024, 4096)
	if err := follower.Start(); err != nil {
		t.Fatal(err)
	}
	defer follower.Stop()

	leaderCoord, err := NewCoordinator("chaos-loss-leader", proxyAddr, redisURL, leader)
	if err != nil {
		t.Fatal(err)
	}
	leader.SetCoordinator(leaderCoord)
	leaderCoord.Start()
	defer leaderCoord.Stop()

	followerCoord, err := NewCoordinator("chaos-loss-follower", follower.Addr(), redisURL, follower)
	if err != nil {
		t.Fatal(err)
	}
	follower.SetCoordinator(followerCoord)
	followerCoord.Start()
	defer followerCoord.Stop()

	topic := "loss-chaos-topic"
	cl := &haCluster{
		leader: leader, follower: follower,
		leaderCoord: leaderCoord, followerCoord: followerCoord,
		topic: topic,
	}
	waitForTopicLeader(t, cl, 20*time.Second)

	leaderSrv, _ := cl.leaderServer()
	followerSrv, _ := cl.followerServer()

	const msgCount = 200
	produceMessages(t, leaderSrv.Addr(), topic, msgCount)

	requireEventually(t, func() bool {
		return partitionOffset(t, followerSrv, topic) == partitionOffset(t, leaderSrv, topic)
	}, 120*time.Second, 500*time.Millisecond, "follower must replicate all messages under packet loss")

	verifyPartitionPayloads(t, followerSrv, topic, msgCount)
	t.Logf("chaos_proof fault=replication_packet_loss loss_pct=5 msgs=%d eventual_consistency=true", msgCount)
}

// Guards moderate Redis latency does not cause election storms.
func TestChaos_RedisLatency_LeaderStability(t *testing.T) {
	skipChaosIntegration(t)

	redisURL, redisCleanup := startChaosRedis(t)
	defer redisCleanup()

	rest := redisURL[len("redis://"):]
	hostPort := rest[:len(rest)-len("/0")]
	// 50ms RTT mimics congested switch latency while staying within leader TTL renew margin.
	proxy, proxyListen := startChaosTCPProxy(t, hostPort, 50*time.Millisecond, 15*time.Millisecond, 0)
	defer proxy.close()
	proxiedRedis := fmt.Sprintf("redis://%s/0", proxyListen)

	cl := startHACluster(t, haClusterOpts{redisURL: proxiedRedis})

	topic := cl.topic
	leaderSrv, leaderCoord := cl.leaderServer()

	requireEventually(t, func() bool {
		return readRedisEpoch(t, leaderCoord, topic) > 0
	}, 20*time.Second, 500*time.Millisecond, "leader epoch must be published to Redis")

	epoch0 := readRedisEpoch(t, leaderCoord, topic)

	cli := client.NewClient(leaderSrv.Addr(), 5*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = cli.Produce(topic, 0, []byte("redis-latency-heartbeat"))
		time.Sleep(2 * time.Second)
	}

	epoch1 := readRedisEpoch(t, leaderCoord, topic)
	delta := int64(epoch1) - int64(epoch0)
	if delta < 0 {
		t.Fatalf("epoch regressed: %d -> %d", epoch0, epoch1)
	}
	if delta > 2 {
		t.Fatalf("election storm: epoch grew by %d (from %d to %d)", delta, epoch0, epoch1)
	}

	t.Logf("chaos_proof fault=redis_latency epoch_start=%d epoch_end=%d delta=%d stable=true", epoch0, epoch1, delta)

	// Stop coordinators before tearing down the Redis proxy they dial through.
	cl.leaderCoord.Stop()
	cl.followerCoord.Stop()
	cl.leader.Stop()
	cl.follower.Stop()
	proxy.close()
}

// Guards failover with partial replication before leader SIGKILL.
func TestChaos_KillLeaderMidReplication(t *testing.T) {
	skipChaosIntegration(t)

	redisURL, redisCleanup := startChaosRedis(t)
	defer redisCleanup()

	bin := ensureBrokerBinary(t)

	leaderDir, err := os.MkdirTemp("", "chaos-kill-leader-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(leaderDir)

	followerDir, err := os.MkdirTemp("", "chaos-kill-follower-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(followerDir)

	leaderAddr := allocFreeTCPAddr(t)
	leaderProc := startBrokerProcess(t, bin, leaderDir, "chaos-kill-leader", redisURL, leaderAddr)

	topic := "kill-mid-repl-topic"
	seedCli := client.NewClient(leaderAddr, 5*time.Second)
	if err := seedCli.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := seedCli.Produce(topic, 0, []byte("bootstrap")); err != nil {
		t.Fatalf("bootstrap produce: %v", err)
	}
	_ = seedCli.Close()

	// Start follower only after the subprocess leader is registered in Redis.
	probeCoord, err := NewCoordinator("chaos-kill-probe", leaderAddr, redisURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireEventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		id, err := probeCoord.rdb.Get(ctx, leaderKey(topicPartitionKey(topic))).Result()
		return err == nil && id == "chaos-kill-leader"
	}, 30*time.Second, 500*time.Millisecond, "subprocess broker must hold redis leadership")
	_ = probeCoord.rdb.Close()

	follower := NewServer(allocFreeTCPAddr(t), followerDir, 10*1024*1024, 4096)
	if err := follower.Start(); err != nil {
		t.Fatal(err)
	}
	defer follower.Stop()

	followerCoord, err := NewCoordinator("chaos-kill-follower", follower.Addr(), redisURL, follower)
	if err != nil {
		t.Fatal(err)
	}
	follower.SetCoordinator(followerCoord)
	followerCoord.Start()
	defer followerCoord.Stop()

	if _, err := follower.getOrCreatePartition(topicPartitionKey(topic)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)

	requireEventually(t, func() bool {
		return !followerCoord.IsLeader(topicPartitionKey(topic))
	}, 15*time.Second, 500*time.Millisecond, "follower must not be leader while replicating")

	var produced atomic.Uint64
	stopProduce := make(chan struct{})
	var produceWg sync.WaitGroup
	produceWg.Add(1)
	go func() {
		defer produceWg.Done()
		cli := client.NewClient(leaderAddr, 2*time.Second)
		if err := cli.Connect(); err != nil {
			return
		}
		defer cli.Close()
		for i := 0; ; i++ {
			select {
			case <-stopProduce:
				return
			default:
			}
			payload := []byte(fmt.Sprintf("kill-msg-%d", i))
			if _, err := cli.Produce(topic, 0, payload); err != nil {
				return
			}
			produced.Store(uint64(i + 1))
		}
	}()

	requireEventually(t, func() bool {
		return partitionOffset(t, follower, topic) >= 10
	}, 30*time.Second, 200*time.Millisecond, "follower must replicate some messages before kill")

	followerBefore := partitionOffset(t, follower, topic)
	leaderProc.kill9(t)

	requireEventually(t, func() bool {
		return followerCoord.IsLeader(topicPartitionKey(topic)) && followerCoord.IsLeaderReady(topicPartitionKey(topic))
	}, 20*time.Second, 500*time.Millisecond, "follower must become ready leader after leader kill")

	close(stopProduce)
	produceWg.Wait()

	totalProduced := produced.Load()
	if totalProduced < followerBefore {
		t.Fatalf("produced %d but follower had %d before kill", totalProduced, followerBefore)
	}

	// Verify replicated prefix; offset 0 is the bootstrap record from subprocess leader.
	verifyKillMidReplicationPrefix(t, follower.Addr(), topic, followerBefore, 1)

	// Failover produce must continue from the follower log tail.
	cli := client.NewClient(follower.Addr(), 5*time.Second)
	cli.SetRedisURL(redisURL)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	tail := partitionOffset(t, follower, topic)
	if _, err := cli.Produce(topic, 0, []byte("after-failover")); err != nil {
		t.Fatalf("produce after failover: %v", err)
	}
	if got := partitionOffset(t, follower, topic); got != tail+1 {
		t.Fatalf("after failover next offset: got %d want %d", got, tail+1)
	}

	metricsBody := scrapeBrokerMetrics(t, follower.HealthAddr())
	pk := topicPartitionKey(topic)
	metricsHasGaugeValue(t, metricsBody, "ad_broker_leader_ready", `topic="`+pk+`"`, "1")
	metricsHasCounterIncrement(t, metricsBody, "ad_broker_produce_total", `status="ok"`)

	t.Logf("chaos_proof fault=kill_leader_mid_replication follower_before=%d total_produced=%d failover_produce=true prefix_intact=true",
		followerBefore, totalProduced)
}

// Guards fencing prevents dual writes after split-brain epoch injection.
func TestChaos_SplitBrain_FencingPreventsDualWrite(t *testing.T) {
	skipChaosIntegration(t)

	cl := startHACluster(t, haClusterOpts{})
	topic := cl.topic
	pk := topicPartitionKey(topic)

	leaderSrv, leaderCoord := cl.leaderServer()
	followerSrv, followerCoord := cl.followerServer()

	epoch, ok := leaderCoord.LeaderEpoch(pk)
	if !ok || epoch == 0 {
		t.Fatal("expected live leader epoch")
	}
	newEpoch := epoch + 1

	// Freeze coordinator loops so injected leadership state is not overwritten.
	leaderCoord.Stop()
	followerCoord.Stop()

	leaderPL, err := leaderSrv.getOrCreatePartition(pk)
	if err != nil {
		t.Fatal(err)
	}
	followerPL, err := followerSrv.getOrCreatePartition(pk)
	if err != nil {
		t.Fatal(err)
	}

	leaderCoord.setLeaderState(pk, true, epoch, true)
	if err := leaderPL.AdvanceFencingEpoch(newEpoch); err != nil {
		t.Fatal(err)
	}
	if err := followerPL.AdvanceFencingEpoch(newEpoch); err != nil {
		t.Fatal(err)
	}
	followerCoord.setLeaderState(pk, true, newEpoch, true)

	staleCli := client.NewClient(leaderSrv.Addr(), 2*time.Second)
	if err := staleCli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer staleCli.Close()

	staleRejected := false
	for i := 0; i < 3; i++ {
		_, err := staleCli.Produce(topic, 0, []byte("stale-brain"))
		if err != nil {
			staleRejected = true
			break
		}
	}
	if !staleRejected {
		t.Fatal("stale leader must not accept writes after fencing floor advanced")
	}

	liveCli := client.NewClient(followerSrv.Addr(), 2*time.Second)
	if err := liveCli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer liveCli.Close()

	off, err := liveCli.Produce(topic, 0, []byte("live-brain"))
	if err != nil {
		t.Fatalf("live leader produce failed: %v", err)
	}
	if _, err := followerPL.AppendFenced(epoch, []byte("direct-stale")); err != log.ErrStaleFencingEpoch {
		t.Fatalf("expected stale direct append rejection, got %v", err)
	}

	t.Logf("chaos_proof fault=split_brain_fencing stale_rejected=true live_offset=%d epoch_floor=%d", off, newEpoch)
}

// Guards async durability RPO is bounded after SIGKILL before background fsync.
func TestChaos_AsyncDurability_RPOOnKill9(t *testing.T) {
	skipChaosIntegration(t)

	redisURL, redisCleanup := startChaosRedis(t)
	defer redisCleanup()

	bin := ensureBrokerBinary(t)
	dir, err := os.MkdirTemp("", "chaos-rpo-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	addr := allocFreeTCPAddr(t)
	topic := "rpo-chaos-topic"
	const msgCount = 50

	proc := startBrokerProcess(t, bin, dir, "chaos-rpo-broker", redisURL, addr,
		"-durability", "async",
		"-flush-interval", "5s",
	)

	cli := client.NewClient(addr, 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < msgCount; i++ {
		payload := []byte(fmt.Sprintf("rpo-msg-%d", i))
		if _, err := cli.Produce(topic, 0, payload); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	_ = cli.Close()

	proc.kill9(t)
	time.Sleep(200 * time.Millisecond)

	restartAddr := allocFreeTCPAddr(t)
	restart := startBrokerProcess(t, bin, dir, "chaos-rpo-broker-2", redisURL, restartAddr,
		"-durability", "async",
		"-flush-interval", "5s",
	)
	_ = restart

	recovered := fetchMessageCount(t, restartAddr, topic)
	if recovered == 0 {
		t.Fatal("expected at least one recovered message after restart")
	}
	if recovered > msgCount {
		t.Fatalf("recovered count %d exceeds produced %d", recovered, msgCount)
	}

	verifyRPOContiguousPrefix(t, restartAddr, topic, recovered)
	t.Logf("chaos_proof fault=async_rpo_kill9 produced=%d recovered=%d contiguous_prefix=true", msgCount, recovered)
}

func verifyKillMidReplicationPrefix(t *testing.T, addr, topic string, count, firstKillOffset uint64) {
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
	var i uint64
	for iter.Next() {
		if i < firstKillOffset {
			i++
			continue
		}
		want := fmt.Sprintf("kill-msg-%d", i-firstKillOffset)
		if string(iter.Payload) != want {
			t.Fatalf("prefix %d: got %q want %q", i, iter.Payload, want)
		}
		if iter.Offset != i {
			t.Fatalf("offset %d: got %d", i, iter.Offset)
		}
		i++
	}
	if i-firstKillOffset < count-firstKillOffset {
		t.Fatalf("expected at least %d kill messages, got %d", count-firstKillOffset, i-firstKillOffset)
	}
}

func verifyRPOContiguousPrefix(t *testing.T, addr, topic string, count uint64) {
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
	var i uint64
	for iter.Next() {
		want := fmt.Sprintf("rpo-msg-%d", i)
		if string(iter.Payload) != want {
			t.Fatalf("rpo %d: got %q want %q", i, iter.Payload, want)
		}
		i++
	}
	if i != count {
		t.Fatalf("expected %d contiguous rpo messages, got %d", count, i)
	}
}
