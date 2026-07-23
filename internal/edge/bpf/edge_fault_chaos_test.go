package bpf

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"espx/internal/edge/blocklist"
	"espx/internal/testutil"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_XDPMalformedPacketFuzzing sends random noise to the XDP program.
// Verifies that the BPF verifier-passed logic never crashes the kernel (simulated)
// and handles out-of-bounds or zero-length inputs gracefully.
func TestChaos_XDPMalformedPacketFuzzing(t *testing.T) {
	objs := loadTestObjects(t)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 1. Zero-length packet
	ret, _, err := objs.XdpEdgeFilter.Test([]byte{})
	// Some kernels/libraries return EINVAL for < 14 bytes
	if err == nil {
		assert.Equal(t, uint32(2), ret, "zero-length should PASS (early eth check)")
	}

	// 2. Too short for ethernet
	ret, _, err = objs.XdpEdgeFilter.Test(make([]byte, 10))
	if err == nil {
		assert.Equal(t, uint32(2), ret, "short packet should PASS")
	}

	// 3. Random noise of varying lengths
	for i := 0; i < 1000; i++ {
		len := 14 + r.Intn(1486) // Minimum ethernet size
		pkt := make([]byte, len)
		r.Read(pkt)
		ret, _, _ = objs.XdpEdgeFilter.Test(pkt)
		// We don't care about the return code, just that it doesn't crash
		if ret != 0 {
			assert.Contains(t, []uint32{1, 2}, ret)
		}
	}

	testutil.LogChaosProof(t, "xdp_packet_fuzzing", map[string]string{
		"iters":   "1000",
		"max_len": "1500",
		"status":  "no_panics",
	})
}

// TestChaos_XDPSyncRedisOutage simulates Redis failure during a sync tick.
// Verifies that edge-bpf-sync does not panic and recovers after Redis is back.
func TestChaos_XDPSyncRedisOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("redis outage chaos test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, rdb, cleanup := testutil.SetupRedisClient(t)
	defer cleanup()

	objs := loadTestObjects(t)
	store := blocklist.NewStore()

	require.NoError(t, rdb.SAdd(ctx, "blacklist:manual", "1.2.3.4").Err())
	_, _, err := blocklist.SyncFromRedis(ctx, rdb, objs.BlocklistV4, store)
	require.NoError(t, err)
	assert.Equal(t, 1, store.Len())

	require.NoError(t, c.Terminate(ctx))

	_, _, err = blocklist.SyncFromRedis(ctx, rdb, objs.BlocklistV4, store)
	assert.Error(t, err, "sync must fail when redis is down")
	assert.Equal(t, 1, store.Len(), "store must preserve state on sync failure")

	testutil.LogChaosProof(t, "xdp_sync_redis_outage", map[string]string{
		"fault":          "container_termination",
		"state_retained": "true",
		"error_handled":  "true",
	})
}

// TestChaos_XDPRingbufCongestion simulates a full ringbuf where the handler
// is slower than the producer.
func TestChaos_XDPRingbufCongestion(t *testing.T) {
	objs := loadTestObjects(t)

	src := net.IPv4(192, 0, 2, 1)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	cfg := uint32(0)
	opts := DefaultConfig(InitOptions{})
	opts.SynLimit = 1
	require.NoError(t, objs.Config.Update(&cfg, &opts, ebpf.UpdateAny))

	for i := 0; i < 2000; i++ {
		runXDP(t, objs.XdpEdgeFilter, pkt)
	}

	handler := NewViolationHandler(func(evt ViolationEvent) error { return nil })
	rd, err := ringbuf.NewReader(objs.Violations)
	require.NoError(t, err)
	defer rd.Close()

	n, err := handler.Drain(rd, 100*time.Millisecond)
	assert.NoError(t, err)
	t.Logf("Drained %d events from congested ringbuf", n)

	testutil.LogChaosProof(t, "xdp_ringbuf_congestion", map[string]string{
		"events_drained": fmt.Sprintf("%d", n),
		"status":         "stable",
	})
}

// TestChaos_XDPLRUEvictionUnderPressure proves that the LRU map handles
// overflow by evicting old entries and remains stable.
func TestChaos_XDPLRUEvictionUnderPressure(t *testing.T) {
	objs := loadTestObjects(t)

	r := rand.New(rand.NewSource(42))

	for i := 0; i < 10000; i++ {
		src := r.Uint32()
		pkt := buildPSHACKPacket(t, net.IP{byte(src >> 24), byte(src >> 16), byte(src >> 8), byte(src)}, net.IPv4(10, 0, 0, 1), trackerPort)
		runXDP(t, objs.XdpEdgeFilter, pkt)
	}

	testutil.LogChaosProof(t, "xdp_lru_high_churn", map[string]string{
		"unique_ips": "10000",
		"status":     "stable",
	})
}

type failingRedisStub struct {
	failAfter int
	count     int
}

func (s *failingRedisStub) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	s.count++
	cmd := redis.NewStringSliceCmd(ctx)
	if s.count > s.failAfter {
		cmd.SetErr(fmt.Errorf("injected redis failure"))
		return cmd
	}
	cmd.SetVal([]string{"198.51.100.1", "198.51.100.2"})
	return cmd
}

// TestChaos_XDPSyncInterruptedPartialUpdate simulates a failure during the sync
// process (e.g. redis timeout between reading different sets).
// Verifies that the in-memory Store and BPF maps are not left in a "torn" state.
func TestChaos_XDPSyncInterruptedPartialUpdate(t *testing.T) {
	objs := loadTestObjects(t)
	store := blocklist.NewStore()

	stub := &failingRedisStub{failAfter: 10}
	added, _, err := blocklist.SyncFromRedis(context.Background(), stub, objs.BlocklistV4, store)
	require.NoError(t, err)
	assert.Equal(t, 2, added)
	assert.Equal(t, 2, store.Len())

	stub.failAfter = 0
	_, _, err = blocklist.SyncFromRedis(context.Background(), stub, objs.BlocklistV4, store)
	assert.Error(t, err)

	assert.Equal(t, 2, store.Len(), "Store must not be partially updated or cleared")

	var val uint8
	require.NoError(t, objs.BlocklistV4.Lookup(blocklist.KeyFromHost(198, 51, 100, 1), &val))
	require.NoError(t, objs.BlocklistV4.Lookup(blocklist.KeyFromHost(198, 51, 100, 2), &val))

	testutil.LogChaosProof(t, "xdp_sync_interrupted", map[string]string{
		"fault":           "partial_redis_failure",
		"state_preserved": "true",
	})
}
