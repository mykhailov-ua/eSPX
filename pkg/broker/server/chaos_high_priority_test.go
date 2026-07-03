package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"espx/pkg/broker/client"
	"espx/pkg/broker/consumer"
)

// Guards follower takes leadership when the leader is partitioned from Redis.
func TestChaos_NetworkPartition_StaleLeaderRejected(t *testing.T) {
	skipChaosIntegration(t)

	cl, proxy := startHAPartitionedCluster(t)
	topic := cl.topic
	pk := topicPartitionKey(topic)

	leaderSrv, _ := cl.leaderServer()
	followerSrv, followerCoord := cl.followerServer()

	produceMessages(t, leaderSrv.Addr(), topic, 20)

	proxy.setBlocked(true)
	t.Cleanup(func() { proxy.setBlocked(false) })

	requireEventually(t, func() bool {
		return followerCoord.IsLeader(pk) && followerCoord.IsLeaderReady(pk)
	}, 25*time.Second, 500*time.Millisecond, "follower must become ready leader after leader redis partition")

	staleCli := client.NewClient(leaderSrv.Addr(), 2*time.Second)
	if err := staleCli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer staleCli.Close()

	staleAccepted := false
	for i := 0; i < 5; i++ {
		_, err := staleCli.Produce(topic, 0, []byte("stale-partition"))
		if err != nil {
			continue
		}
		staleAccepted = true
	}

	liveCli := client.NewClient(followerSrv.Addr(), 2*time.Second)
	liveCli.SetRedisURL(cl.redisURL)
	if err := liveCli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer liveCli.Close()

	if _, err := liveCli.Produce(topic, 0, []byte("live-after-partition")); err != nil {
		t.Fatalf("new leader produce failed: %v", err)
	}

	verifyPartitionPayloads(t, followerSrv, topic, 20)
	if got := partitionOffset(t, followerSrv, topic); got != 21 {
		t.Fatalf("follower next offset: got %d want 21", got)
	}

	iter, err := liveCli.Fetch(topic, 0, 0, 32*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	for iter.Next() {
		if string(iter.Payload) == "stale-partition" {
			t.Fatal("follower log must not contain stale-partition writes from partitioned leader")
		}
	}

	t.Logf("chaos_proof fault=redis_network_partition stale_local_accept=%v follower_msgs=21 authoritative=true", staleAccepted)
}

// Guards a frozen leader process cannot resume writes after lease expiry and follower election.
func TestChaos_LeaseExpiry_FrozenLeaderRejected(t *testing.T) {
	skipChaosIntegration(t)

	redisURL, redisCleanup := startChaosRedis(t)
	defer redisCleanup()

	bin := ensureBrokerBinary(t)
	leaderDir, err := os.MkdirTemp("", "chaos-freeze-leader-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(leaderDir)

	followerDir, err := os.MkdirTemp("", "chaos-freeze-follower-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(followerDir)

	leaderAddr := allocFreeTCPAddr(t)
	leaderProc := startBrokerProcess(t, bin, leaderDir, "chaos-freeze-leader", redisURL, leaderAddr)

	topic := "freeze-lease-topic"
	pk := topicPartitionKey(topic)
	seedCli := client.NewClient(leaderAddr, 5*time.Second)
	if err := seedCli.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := seedCli.Produce(topic, 0, []byte("bootstrap")); err != nil {
		t.Fatalf("bootstrap produce: %v", err)
	}
	_ = seedCli.Close()

	probeCoord, err := NewCoordinatorWithConfig("chaos-freeze-probe", leaderAddr, redisURL, nil, fastCoordConfig())
	if err != nil {
		t.Fatal(err)
	}
	requireEventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		id, err := probeCoord.rdb.Get(ctx, leaderKey(pk)).Result()
		return err == nil && id == "chaos-freeze-leader"
	}, 30*time.Second, 500*time.Millisecond, "subprocess leader must register in redis")
	_ = probeCoord.rdb.Close()

	follower := NewServer(allocFreeTCPAddr(t), followerDir, 10*1024*1024, 4096)
	if err := follower.Start(); err != nil {
		t.Fatal(err)
	}
	defer follower.Stop()

	followerCoord, err := NewCoordinatorWithConfig("chaos-freeze-follower", follower.Addr(), redisURL, follower, fastCoordConfig())
	if err != nil {
		t.Fatal(err)
	}
	follower.SetCoordinator(followerCoord)
	followerCoord.Start()
	defer followerCoord.Stop()

	if _, err := follower.getOrCreatePartition(pk); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	preCli := client.NewClient(leaderAddr, 5*time.Second)
	if err := preCli.Connect(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 15; i++ {
		if _, err := preCli.Produce(topic, 0, []byte(fmt.Sprintf("freeze-%d", i))); err != nil {
			t.Fatalf("freeze produce %d: %v", i, err)
		}
	}
	_ = preCli.Close()

	if leaderProc.cmd.Process == nil {
		t.Fatal("leader process not running")
	}
	if err := syscall.Kill(leaderProc.cmd.Process.Pid, syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP leader: %v", err)
	}
	t.Cleanup(func() {
		if leaderProc.cmd.Process != nil {
			_ = syscall.Kill(leaderProc.cmd.Process.Pid, syscall.SIGCONT)
		}
	})

	requireEventually(t, func() bool {
		return followerCoord.IsLeader(pk) && followerCoord.IsLeaderReady(pk)
	}, 25*time.Second, 500*time.Millisecond, "follower must become leader while leader is frozen")

	if err := syscall.Kill(leaderProc.cmd.Process.Pid, syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT leader: %v", err)
	}
	time.Sleep(3 * time.Second)

	staleCli := client.NewClient(leaderAddr, 2*time.Second)
	if err := staleCli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer staleCli.Close()

	staleRejected := false
	for i := 0; i < 5; i++ {
		_, err := staleCli.Produce(topic, 0, []byte("stale-after-freeze"))
		if err != nil {
			staleRejected = true
			break
		}
	}
	if !staleRejected {
		t.Fatal("resumed stale leader must not accept writes after follower election")
	}

	liveCli := client.NewClient(follower.Addr(), 2*time.Second)
	liveCli.SetRedisURL(redisURL)
	if err := liveCli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer liveCli.Close()

	if _, err := liveCli.Produce(topic, 0, []byte("live-after-freeze")); err != nil {
		t.Fatalf("new leader produce: %v", err)
	}

	leaderProc.kill9(t)
	verifyFreezeLeasePrefix(t, follower.Addr(), topic, 17)
	t.Logf("chaos_proof fault=lease_expiry_sigstop stale_rejected=%v failover_produce=true", staleRejected)
}

// Guards client Produce redirects via Redis after leader failover without manual addr update.
func TestChaos_ClientRedirect_AfterFailover(t *testing.T) {
	skipChaosIntegration(t)

	cl := startHAClusterOrdered(t)
	topic := cl.topic
	pk := topicPartitionKey(topic)

	leaderSrv := cl.leader
	leaderCoord := cl.leaderCoord
	followerSrv := cl.follower
	followerCoord := cl.followerCoord

	const msgCount = 12
	produceMessages(t, leaderSrv.Addr(), topic, msgCount)
	requireEventually(t, func() bool {
		return partitionOffset(t, cl.follower, topic) >= msgCount
	}, 30*time.Second, 300*time.Millisecond, "follower must replicate all messages before failover")

	cli := client.NewClient(leaderSrv.Addr(), 5*time.Second)
	cli.SetRedisURL(cl.redisURL)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	leaderCoord.Stop()
	leaderSrv.Stop()

	requireEventually(t, func() bool {
		return followerCoord.IsLeader(pk) && followerCoord.IsLeaderReady(pk)
	}, 20*time.Second, 500*time.Millisecond, "follower must become ready leader")

	tail := partitionOffset(t, followerSrv, topic)
	off, err := cli.Produce(topic, 0, []byte("redirect-after-failover"))
	if err != nil {
		t.Fatalf("produce via redis redirect failed: %v", err)
	}
	if off != tail {
		t.Fatalf("redirect produce offset: got %d want %d", off, tail)
	}

	verifyPartitionPayloads(t, followerSrv, topic, msgCount)
	if got := partitionOffset(t, followerSrv, topic); got != tail+1 {
		t.Fatalf("follower next offset after redirect produce: got %d want %d", got, tail+1)
	}
	t.Logf("chaos_proof fault=client_redis_failover stale_addr=true redirect_produce=true offset=%d", off)
}

// Guards consumer resumes from committed offset after leader failover and continues reading.
func TestChaos_ConsumerResume_AfterLeaderKill(t *testing.T) {
	skipChaosIntegration(t)

	cl := startHAClusterOrdered(t)
	topic := cl.topic
	pk := topicPartitionKey(topic)

	leaderSrv := cl.leader
	leaderCoord := cl.leaderCoord
	followerSrv := cl.follower
	followerCoord := cl.followerCoord

	const preKill = 8
	const postKill = 8

	preCli := client.NewClient(leaderSrv.Addr(), 5*time.Second)
	if err := preCli.Connect(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < preKill; i++ {
		if _, err := preCli.Produce(topic, 0, []byte(fmt.Sprintf("pre-%d", i))); err != nil {
			t.Fatalf("pre produce %d: %v", i, err)
		}
	}
	_ = preCli.Close()

	requireEventually(t, func() bool {
		return partitionOffset(t, followerSrv, topic) >= preKill
	}, 30*time.Second, 300*time.Millisecond, "follower must replicate pre-kill messages")

	var seenMu sync.Mutex
	seen := make(map[string]struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons := consumer.New(consumer.Config{
		BrokerAddr: leaderSrv.Addr(),
		RedisURL:   cl.redisURL,
		Topic:      topic,
		Group:      "chaos-resume-group",
		MaxBytes:   1024 * 1024,
		Timeout:    5 * time.Second,
		IdleWait:   100 * time.Millisecond,
	}, func(payload []byte, _ uint64) error {
		seenMu.Lock()
		seen[string(payload)] = struct{}{}
		n := len(seen)
		seenMu.Unlock()
		if n >= preKill+postKill {
			cancel()
		}
		return nil
	})

	var consErr atomic.Value
	var consWg sync.WaitGroup
	consWg.Add(1)
	go func() {
		defer consWg.Done()
		if err := cons.Run(ctx); err != nil && err != context.Canceled {
			consErr.Store(err)
		}
	}()

	requireEventually(t, func() bool {
		seenMu.Lock()
		defer seenMu.Unlock()
		return len(seen) >= preKill
	}, 45*time.Second, 200*time.Millisecond, "consumer must drain pre-kill messages before leader stop")

	leaderCoord.Stop()
	leaderSrv.Stop()

	requireEventually(t, func() bool {
		return followerCoord.IsLeader(pk) && followerCoord.IsLeaderReady(pk)
	}, 20*time.Second, 500*time.Millisecond, "follower must become ready leader")

	postCli := client.NewClient(followerSrv.Addr(), 5*time.Second)
	postCli.SetRedisURL(cl.redisURL)
	if err := postCli.Connect(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < postKill; i++ {
		if _, err := postCli.Produce(topic, 0, []byte(fmt.Sprintf("post-%d", i))); err != nil {
			t.Fatalf("post failover produce %d: %v", i, err)
		}
	}
	_ = postCli.Close()
	time.Sleep(500 * time.Millisecond)

	requireEventually(t, func() bool {
		seenMu.Lock()
		defer seenMu.Unlock()
		return len(seen) >= preKill+postKill
	}, 90*time.Second, 200*time.Millisecond, "consumer must read all pre and post failover messages")

	consWg.Wait()
	if v := consErr.Load(); v != nil {
		t.Fatalf("consumer error: %v", v)
	}

	commitCli := client.NewClient(followerSrv.Addr(), 5*time.Second)
	if err := commitCli.Connect(); err != nil {
		t.Fatal(err)
	}
	off, commitErr := commitCli.CommittedOffset(topic, 0, "chaos-resume-group")
	_ = commitCli.Close()
	if commitErr != nil {
		t.Fatalf("committed offset: %v", commitErr)
	}
	if off < uint64(preKill+postKill) {
		t.Fatalf("committed offset %d want >= %d", off, preKill+postKill)
	}

	t.Logf("chaos_proof fault=consumer_resume_leader_kill seen=%d committed_ok=true", preKill+postKill)
}

func verifyFreezeLeasePrefix(t *testing.T, addr, topic string, wantCount int) {
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
	var n int
	for iter.Next() {
		n++
	}
	if n < wantCount {
		t.Fatalf("expected at least %d messages, got %d", wantCount, n)
	}
}
