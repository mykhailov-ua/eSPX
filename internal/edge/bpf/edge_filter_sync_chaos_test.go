package bpf

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/edge/blocklist"
	"espx/internal/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const scheduledSyncInterval = 5 * time.Second

// TestChaos_XDPEarlySyncAheadOfSchedule models edge-bpf-sync post-violation rewrite:
// autoban lands in Redis and SyncFromRedis runs before the scheduled SYNC_INTERVAL tick,
// while concurrent scheduled re-sync goroutines hammer the same Store + BPF map.
//
// Hypothesis: victim IP is XDP-dropped after early sync; control IP stays PASS; deny snapshot
// stays consistent under 24 concurrent sync workers (-race clean).
func TestChaos_XDPEarlySyncAheadOfSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("xdp early sync chaos test")
	}

	ctx := context.Background()
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	objs := loadTestObjects(t)
	store := blocklist.NewStore()

	victim := net.IPv4(203, 0, 113, 150)
	control := net.IPv4(10, 20, 30, 50)
	victimPkt := buildSYNPacket(t, victim, net.IPv4(10, 0, 0, 1), trackerPort)
	controlPkt := buildSYNPacket(t, control, net.IPv4(10, 0, 0, 1), trackerPort)

	require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, victimPkt), "victim pre-ban")
	require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, controlPkt), "control pre-ban")

	scheduledAt := time.Now().Add(scheduledSyncInterval)
	earlyDone := make(chan time.Duration, 1)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _, err := blocklist.SyncFromRedis(ctx, rdb, objs.BlocklistV4, store)
				if err != nil {
					t.Errorf("scheduled sync worker %d: %v", id, err)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}

	// Post-violation path: RecordAutoBan + immediate sync (ahead of SYNC_INTERVAL).
	require.NoError(t, blocklist.RecordAutoBan(ctx, rdb, victim.String(), 5*time.Minute))
	earlyStart := time.Now()
	_, _, err := blocklist.SyncFromRedis(ctx, rdb, objs.BlocklistV4, store)
	require.NoError(t, err)
	earlyLatency := time.Since(earlyStart)
	earlyDone <- earlyLatency

	require.True(t, time.Now().Before(scheduledAt),
		"early sync must complete before scheduled %s tick", scheduledSyncInterval)

	require.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, victimPkt), "victim post-early-sync")
	require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, controlPkt), "control post-early-sync")

	close(stop)
	wg.Wait()

	v4 := victim.To4()
	require.NotNil(t, v4)
	key := blocklist.KeyFromHost(v4[0], v4[1], v4[2], v4[3])
	var marker uint8
	require.NoError(t, objs.BlocklistV4.Lookup(key, &marker))
	assert.Equal(t, uint8(1), marker)
	assert.GreaterOrEqual(t, store.Len(), 1)

	ms := int(earlyLatency.Round(time.Millisecond) / time.Millisecond)
	if ms < 1 {
		ms = 1
	}

	testutil.LogChaosProof(t, "xdp_early_sync_ahead_of_schedule", map[string]string{
		"harness":              "redis_bpf_map",
		"early_sync_ms":        fmt.Sprintf("%d", ms),
		"scheduled_interval_s": fmt.Sprintf("%d", int(scheduledSyncInterval/time.Second)),
		"concurrent_syncers":   "24",
		"victim_dropped":       "true",
		"control_stable":       "true",
		"deny_entries":         fmt.Sprintf("%d", store.Len()),
	})
}
