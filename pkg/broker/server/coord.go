package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/metrics"
	"espx/pkg/broker/client"
	"espx/pkg/broker/log"
	"espx/pkg/broker/protocol"
	"github.com/redis/go-redis/v9"
)

// topicLeaderState tracks local leadership, fencing epoch, and write readiness after failover.
type topicLeaderState struct {
	isLeader bool
	epoch    uint64
	ready    bool
}

// Coordinator elects per-topic leaders in Redis and tails the leader log on followers.
type Coordinator struct {
	nodeID        string
	tcpAddr       string
	rdb           redis.UniversalClient
	server        *Server
	cfg           CoordConfig
	closeChan     chan struct{}
	closeOnce     sync.Once
	wg            sync.WaitGroup
	leaders       atomic.Pointer[map[string]topicLeaderState]
	renewFailures map[string]int
	renewMu       sync.Mutex
}

// NewCoordinator wires Redis leader election to the local broker for HA produce routing.
func NewCoordinator(nodeID string, tcpAddr string, redisURL string, server *Server) (*Coordinator, error) {
	return NewCoordinatorWithConfig(nodeID, tcpAddr, redisURL, server, DefaultCoordConfig())
}

// NewCoordinatorWithConfig wires Redis leader election with explicit lease tuning.
func NewCoordinatorWithConfig(nodeID string, tcpAddr string, redisURL string, server *Server, cfg CoordConfig) (*Coordinator, error) {
	rdb, err := openCoordRedis(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open redis: %w", err)
	}

	c := &Coordinator{
		nodeID:        nodeID,
		tcpAddr:       tcpAddr,
		rdb:           rdb,
		server:        server,
		cfg:           cfg.normalized(),
		closeChan:     make(chan struct{}),
		renewFailures: make(map[string]int),
	}
	initMap := make(map[string]topicLeaderState)
	c.leaders.Store(&initMap)
	return c, nil
}

// openCoordRedis returns a standalone client or Sentinel failover client when lab env vars are set.
func openCoordRedis(redisURL string) (redis.UniversalClient, error) {
	master := os.Getenv("BROKER_REDIS_SENTINEL_MASTER")
	if master != "" {
		addrs := strings.Split(os.Getenv("BROKER_REDIS_SENTINEL_ADDRS"), ",")
		trimmed := make([]string, 0, len(addrs))
		for _, a := range addrs {
			a = strings.TrimSpace(a)
			if a != "" {
				trimmed = append(trimmed, a)
			}
		}
		if len(trimmed) == 0 {
			return nil, fmt.Errorf("BROKER_REDIS_SENTINEL_ADDRS is empty")
		}
		var pwd string
		if opts, err := redis.ParseURL(redisURL); err == nil {
			pwd = opts.Password
		}
		if pwd == "" {
			pwd = os.Getenv("BROKER_REDIS_PASSWORD")
		}
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       master,
			SentinelAddrs:    trimmed,
			Password:         pwd,
			SentinelPassword: pwd,
		}), nil
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opts), nil
}

// Start runs heartbeat and leader-election loops so the node joins cluster coordination.
func (c *Coordinator) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runHeartbeatLoop()
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runCoordinationLoop()
	}()
}

// Redis returns the coordination client used for leader election and topic registry sync.
func (c *Coordinator) Redis() redis.UniversalClient {
	return c.rdb
}

// Stop tears down coordination goroutines and removes this broker from the discovery registry.
func (c *Coordinator) Stop() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
	c.wg.Wait()
	_ = c.rdb.Close()
}

// IsLeader reports whether this node may accept writes for a topic under HA mode.
func (c *Coordinator) IsLeader(topic string) bool {
	m := c.leaders.Load()
	if m == nil {
		return false
	}
	return (*m)[topic].isLeader
}

// LeaderEpoch returns the fencing epoch for the current leader term on this node.
func (c *Coordinator) LeaderEpoch(topic string) (uint64, bool) {
	m := c.leaders.Load()
	if m == nil {
		return 0, false
	}
	st := (*m)[topic]
	if !st.isLeader || st.epoch == 0 {
		return 0, false
	}
	return st.epoch, true
}

// IsLeaderReady reports whether the elected leader has caught up to the published log high-water mark.
func (c *Coordinator) IsLeaderReady(topic string) bool {
	m := c.leaders.Load()
	if m == nil {
		return false
	}
	st := (*m)[topic]
	return st.isLeader && st.ready
}

// PublishLogHWM records the leader's next offset so followers can verify readiness on failover.
func (c *Coordinator) PublishLogHWM(topic string, hwm uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.rdb.Set(ctx, logHWMKey(topic), strconv.FormatUint(hwm, 10), 0).Err()
}

// HasLeader checks Redis for an elected leader so followers can reject stale produce attempts.
func (c *Coordinator) HasLeader(topic string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	exists, err := c.rdb.Exists(ctx, leaderKey(topic)).Result()
	return exists > 0, err
}

func leaderKey(topic string) string {
	return "espx:topics:" + topic + ":leader"
}

func leaderEpochKey(topic string) string {
	return "espx:topics:" + topic + ":leader_epoch"
}

func logHWMKey(topic string) string {
	return "espx:topics:" + topic + ":log_hwm"
}

// runHeartbeatLoop publishes this broker's TCP address so clients and followers can find the leader.
func (c *Coordinator) runHeartbeatLoop() {
	interval := c.cfg.Interval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lease := c.cfg.LeaseTTL

	for {
		select {
		case <-c.closeChan:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = c.rdb.Del(ctx, "espx:brokers:"+c.nodeID).Err()
			cancel()
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = c.rdb.Set(ctx, "espx:brokers:"+c.nodeID, c.tcpAddr, lease).Err()
			cancel()
		}
	}
}

// runCoordinationLoop acquires topic leadership or starts tailing the elected leader's log.
func (c *Coordinator) runCoordinationLoop() {
	interval := c.cfg.Interval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	replications := make(map[string]chan struct{})

	for {
		select {
		case <-c.closeChan:
			for _, stopCh := range replications {
				close(stopCh)
			}
			return
		case <-ticker.C:
			var topics []string
			c.server.topics.Range(func(key, _ any) bool {
				topics = append(topics, key.(string))
				return true
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			for _, topic := range topics {
				c.coordTopic(ctx, topic, replications)
			}
			cancel()
		}
	}
}

func (c *Coordinator) coordTopic(ctx context.Context, topic string, replications map[string]chan struct{}) {
	lKey := leaderKey(topic)
	eKey := leaderEpochKey(topic)
	lease := c.cfg.LeaseTTL

	ok, err := c.rdb.SetNX(ctx, lKey, c.nodeID, lease).Result()
	if err != nil {
		return
	}

	if ok {
		epoch, bumped, err := c.acquireEpoch(ctx, topic, eKey)
		if err != nil {
			_ = c.rdb.Del(ctx, lKey).Err()
			return
		}
		if stopCh, exists := replications[topic]; exists {
			close(stopCh)
			delete(replications, topic)
		}
		_ = c.rdb.Expire(ctx, lKey, lease).Err()
		c.clearRenewFailures(topic)
		c.onAcquiredLeadership(ctx, topic, epoch, bumped)
		return
	}

	currentLeader, err := c.rdb.Get(ctx, lKey).Result()
	if err == nil && currentLeader == c.nodeID {
		epoch := c.readEpoch(ctx, topic)
		pl, plErr := c.server.getOrCreatePartition(topic)
		if plErr == nil {
			c.PublishLogHWM(topic, pl.NextOffset())
		}
		if !c.renewLease(ctx, topic, lKey) {
			clusterEpoch := c.readEpoch(ctx, topic)
			c.demoteTopic(topic, clusterEpoch)
			c.updateTopicMetrics(ctx, topic)
			return
		}
		c.setLeaderState(topic, true, epoch, true)
		c.updateTopicMetrics(ctx, topic)
		return
	}

	clusterEpoch := c.readEpoch(ctx, topic)
	if c.IsLeader(topic) {
		c.demoteTopic(topic, clusterEpoch)
	}
	if _, exists := replications[topic]; exists {
		return
	}
	stopCh := make(chan struct{})
	replications[topic] = stopCh
	c.wg.Add(1)
	go func(t string, leaderID string, sCh chan struct{}) {
		defer c.wg.Done()
		c.replicate(t, leaderID, sCh)
	}(topic, currentLeader, stopCh)

	c.updateTopicMetrics(ctx, topic)
}

func (c *Coordinator) acquireEpoch(ctx context.Context, topic, eKey string) (uint64, bool, error) {
	lastWinner, _ := c.rdb.Get(ctx, leaderLastWinnerKey(topic)).Result()
	lastSince, _ := c.rdb.Get(ctx, leaderSinceKey(topic)).Result()
	if lastWinner == c.nodeID && lastSince != "" {
		if sinceUnix, err := strconv.ParseInt(lastSince, 10, 64); err == nil {
			elapsed := time.Since(time.Unix(sinceUnix, 0))
			if elapsed < c.cfg.DebounceWindow {
				epoch := c.readEpoch(ctx, topic)
				if epoch > 0 {
					now := strconv.FormatInt(time.Now().Unix(), 10)
					_ = c.rdb.Set(ctx, leaderSinceKey(topic), now, 0).Err()
					_ = c.rdb.Set(ctx, leaderLastWinnerKey(topic), c.nodeID, 0).Err()
					return epoch, false, nil
				}
			}
		}
	}
	epoch, err := c.rdb.Incr(ctx, eKey).Result()
	if err != nil {
		return 0, false, err
	}
	now := strconv.FormatInt(time.Now().Unix(), 10)
	_ = c.rdb.Set(ctx, leaderSinceKey(topic), now, 0).Err()
	_ = c.rdb.Set(ctx, leaderLastWinnerKey(topic), c.nodeID, 0).Err()
	return uint64(epoch), true, nil
}

func (c *Coordinator) renewLease(ctx context.Context, topic, lKey string) bool {
	lease := c.cfg.LeaseTTL
	ok, err := c.rdb.Expire(ctx, lKey, lease).Result()
	if err != nil || !ok {
		c.recordRenewFailure(topic)
		return false
	}
	currentLeader, err := c.rdb.Get(ctx, lKey).Result()
	if err != nil || currentLeader != c.nodeID {
		c.recordRenewFailure(topic)
		return false
	}
	c.clearRenewFailures(topic)
	return true
}

func (c *Coordinator) recordRenewFailure(topic string) {
	c.renewMu.Lock()
	defer c.renewMu.Unlock()
	c.renewFailures[topic]++
	if c.renewFailures[topic] >= c.cfg.RenewFailThreshold {
		slog.Warn("Leader lease renew failed repeatedly; stepping down proactively",
			"topic", topic, "node_id", c.nodeID, "failures", c.renewFailures[topic])
	}
}

func (c *Coordinator) clearRenewFailures(topic string) {
	c.renewMu.Lock()
	defer c.renewMu.Unlock()
	delete(c.renewFailures, topic)
}

// updateTopicMetrics publishes HA gauges for dashboards and split-brain alerts.
func (c *Coordinator) updateTopicMetrics(ctx context.Context, topic string) {
	hwm := c.readLogHWM(ctx, topic)
	local := uint64(0)
	pl, err := c.server.getOrCreatePartition(topic)
	if err == nil {
		local = pl.NextOffset()
	}
	lag := float64(0)
	if hwm > local {
		lag = float64(hwm - local)
	}
	metrics.BrokerReplicationLag.WithLabelValues(topic).Set(lag)

	m := c.leaders.Load()
	st := (*m)[topic]
	leader := float64(0)
	ready := float64(0)
	epoch := float64(0)
	if st.isLeader {
		leader = 1
		if st.ready {
			ready = 1
		}
		epoch = float64(st.epoch)
	}
	metrics.BrokerActiveLeader.WithLabelValues(topic, c.nodeID).Set(leader)
	metrics.BrokerLeaderReady.WithLabelValues(topic, c.nodeID).Set(ready)
	metrics.BrokerLeaderEpoch.WithLabelValues(topic, c.nodeID).Set(epoch)
}

func (c *Coordinator) readEpoch(ctx context.Context, topic string) uint64 {
	val, err := c.rdb.Get(ctx, leaderEpochKey(topic)).Result()
	if err != nil {
		return 0
	}
	epoch, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0
	}
	return epoch
}

func (c *Coordinator) readLogHWM(ctx context.Context, topic string) uint64 {
	val, err := c.rdb.Get(ctx, logHWMKey(topic)).Result()
	if err != nil {
		return 0
	}
	hwm, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0
	}
	return hwm
}

func (c *Coordinator) onAcquiredLeadership(ctx context.Context, topic string, epoch uint64, bumped bool) {
	if bumped {
		metrics.BrokerLeaderElectionTotal.WithLabelValues(topic).Inc()
	}

	pl, err := c.server.getOrCreatePartition(topic)
	if err != nil {
		c.setLeaderState(topic, true, epoch, true)
		slog.Info("Acquired topic leadership", "topic", topic, "epoch", epoch)
		return
	}

	local := pl.NextOffset()
	hwm := c.readLogHWM(ctx, topic)
	ready := local >= hwm
	c.setLeaderState(topic, true, epoch, ready)
	c.PublishLogHWM(topic, local)
	slog.Info("Acquired topic leadership", "topic", topic, "epoch", epoch, "local", local, "hwm", hwm, "ready", ready)

	if !ready {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.recoverLeaderReadiness(topic, hwm, time.Now())
		}()
	}
	c.updateTopicMetrics(ctx, topic)
}

// recoverLeaderReadiness waits for replication catch-up or accepts a bounded gap after failover timeout.
func (c *Coordinator) recoverLeaderReadiness(topic string, targetHWM uint64, started time.Time) {
	const timeout = 5 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		pl, err := c.server.getOrCreatePartition(topic)
		if err == nil && pl.NextOffset() >= targetHWM {
			c.setLeaderReady(topic, true)
			metrics.BrokerReplicationCatchupSeconds.WithLabelValues(topic).Observe(time.Since(started).Seconds())
			slog.Info("Leader ready after catch-up", "topic", topic, "offset", pl.NextOffset())
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	pl, err := c.server.getOrCreatePartition(topic)
	if err != nil {
		c.setLeaderReady(topic, true)
		metrics.BrokerReplicationCatchupSeconds.WithLabelValues(topic).Observe(time.Since(started).Seconds())
		return
	}
	local := pl.NextOffset()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_ = c.rdb.Set(ctx, logHWMKey(topic), strconv.FormatUint(local, 10), 0).Err()
	cancel()
	c.setLeaderReady(topic, true)
	metrics.BrokerReplicationCatchupSeconds.WithLabelValues(topic).Observe(time.Since(started).Seconds())
	slog.Warn("Leader readiness accepted with replication gap",
		"topic", topic, "local", local, "target_hwm", targetHWM)
}

// demoteTopic clears local leadership and raises the partition fencing floor to the cluster epoch.
func (c *Coordinator) demoteTopic(topic string, clusterEpoch uint64) {
	c.setLeaderState(topic, false, 0, false)
	if clusterEpoch == 0 {
		return
	}
	pl, err := c.server.getOrCreatePartition(topic)
	if err != nil {
		return
	}
	if err := pl.AdvanceFencingEpoch(clusterEpoch); err != nil {
		slog.Warn("Failed to advance fencing epoch on demotion", "topic", topic, "epoch", clusterEpoch, "error", err)
	}
}

// setLeaderState publishes leadership and epoch changes without locking readers on the produce path.
func (c *Coordinator) setLeaderState(topic string, isLeader bool, epoch uint64, ready bool) {
	for {
		old := c.leaders.Load()
		newMap := make(map[string]topicLeaderState, len(*old)+1)
		for k, v := range *old {
			newMap[k] = v
		}
		if !isLeader {
			ready = false
		}
		newMap[topic] = topicLeaderState{isLeader: isLeader, epoch: epoch, ready: ready}
		if c.leaders.CompareAndSwap(old, &newMap) {
			return
		}
	}
}

func (c *Coordinator) setLeaderReady(topic string, ready bool) {
	for {
		old := c.leaders.Load()
		st := (*old)[topic]
		if !st.isLeader {
			return
		}
		newMap := make(map[string]topicLeaderState, len(*old))
		for k, v := range *old {
			newMap[k] = v
		}
		st.ready = ready
		newMap[topic] = st
		if c.leaders.CompareAndSwap(old, &newMap) {
			return
		}
	}
}

// replicate tails the leader partition log so followers serve consistent fetch offsets after failover.
func (c *Coordinator) replicate(topic string, leaderID string, stopCh chan struct{}) {
	slog.Info("Starting replication", "topic", topic, "leader", leaderID)
	defer slog.Info("Stopped replication", "topic", topic)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var cli *client.Client
	var currentAddr string

	defer func() {
		if cli != nil {
			_ = cli.Close()
		}
	}()

	for {
		select {
		case <-stopCh:
			return
		case <-c.closeChan:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			leaderAddr, err := c.rdb.Get(ctx, "espx:brokers:"+leaderID).Result()
			cancel()
			if err != nil {
				if cli != nil {
					_ = cli.Close()
					cli = nil
				}
				continue
			}

			if cli == nil || leaderAddr != currentAddr {
				if cli != nil {
					_ = cli.Close()
				}
				cli = client.NewClient(leaderAddr, time.Second)
				currentAddr = leaderAddr
				if err := cli.Connect(); err != nil {
					_ = cli.Close()
					cli = nil
					continue
				}
			}

			pl, err := c.server.getOrCreatePartition(topic)
			if err != nil {
				continue
			}

			nextOffset := pl.NextOffset()

			topicName, part := protocol.ParseTopicPartitionID(topic)
			iter, fetchErr := cli.Fetch(topicName, part, nextOffset, 65536)
			if fetchErr == nil {
				for iter.Next() {
					if _, err = pl.AppendReplicatedAt(iter.Offset, iter.Payload); err != nil {
						if errors.Is(err, log.ErrReplicationGap) {
							slog.Warn("Replication gap detected, halting batch",
								"topic", topic,
								"expected", pl.NextOffset(),
								"got", iter.Offset,
							)
						}
						fetchErr = err
						break
					}
				}
			}

			if fetchErr != nil {
				_ = cli.Close()
				cli = nil
				if errors.Is(fetchErr, log.ErrReplicationGap) {
					recordReplicationError(topic, "gap")
				} else if !errors.Is(fetchErr, io.EOF) {
					recordReplicationError(topic, "fetch")
				}
				if !errors.Is(fetchErr, io.EOF) {
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}
}
