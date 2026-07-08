package ads

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// nodeID identifies this process in fast UUID generation.
var nodeID uint16

// idSequence is the per-process counter mixed into fast UUIDs.
var idSequence uint64

// cachedUnixMilli avoids time.Now syscalls for TTC and timestamp fields on the hot path.
var cachedUnixMilli atomic.Int64

// cachedUnixMilliAny mirrors cachedUnixMilli for zero-alloc Lua argv boxing.
var cachedUnixMilliAny atomic.Value

// cachedNowUTC holds wall time refreshed once per second for schedule and pacing checks.
var cachedNowUTC atomic.Pointer[time.Time]

// clockRefreshPaused freezes cached wall-clock updates for deterministic chaos tests.
var clockRefreshPaused atomic.Bool

// SetClockRefreshPaused stops background cachedUnixMilli/cachedNowUTC refresh (tests only).
func SetClockRefreshPaused(paused bool) {
	clockRefreshPaused.Store(paused)
}

// storeCachedNowUTC snapshots the current UTC instant for cached time readers.
func storeCachedNowUTC() {
	t := time.Now().UTC()
	cachedNowUTC.Store(&t)
}

// CachedTimeUTC returns wall time in UTC without a syscall on the filter hot path.
func CachedTimeUTC() time.Time {
	if p := cachedNowUTC.Load(); p != nil {
		return *p
	}
	return time.Now().UTC()
}

// CachedTimeIn converts the cached UTC instant into a campaign timezone.
func CachedTimeIn(loc *time.Location) time.Time {
	if loc == nil || loc == time.UTC {
		return CachedTimeUTC()
	}
	return CachedTimeUTC().In(loc)
}

// init seeds fast UUID node identity and starts background time refresh goroutines.
func init() {
	hostname, _ := os.Hostname()
	h := uint32(os.Getpid())
	for _, c := range hostname {
		h = h*31 + uint32(c)
	}
	nodeID = uint16(h ^ (h >> 16))

	cachedUnixMilli.Store(time.Now().UnixMilli())
	cachedUnixMilliAny.Store(cachedUnixMilli.Load())
	storeCachedNowUTC()
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if clockRefreshPaused.Load() {
				continue
			}
			ms := time.Now().UnixMilli()
			cachedUnixMilli.Store(ms)
			cachedUnixMilliAny.Store(ms)
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if clockRefreshPaused.Load() {
				continue
			}
			storeCachedNowUTC()
		}
	}()
}

// NewFastUUID generates click IDs without crypto/rand or uuid library overhead.
func NewFastUUID() uuid.UUID {
	seq := atomic.AddUint64(&idSequence, 1)
	now := cachedUnixMilli.Load()

	var u uuid.UUID

	u[0] = byte(now >> 40)
	u[1] = byte(now >> 32)
	u[2] = byte(now >> 24)
	u[3] = byte(now >> 16)
	u[4] = byte(now >> 8)
	u[5] = byte(now)

	u[6] = byte(seq >> 48)
	u[7] = byte(seq >> 40)

	u[8] = byte(nodeID >> 8)
	u[9] = byte(nodeID)

	u[10] = byte(seq >> 40)
	u[11] = byte(seq >> 32)
	u[12] = byte(seq >> 24)
	u[13] = byte(seq >> 16)
	u[14] = byte(seq >> 8)
	u[15] = byte(seq)

	u[6] = (u[6] & 0x0f) | 0x70
	u[8] = (u[8] & 0x3f) | 0x80

	return u
}
