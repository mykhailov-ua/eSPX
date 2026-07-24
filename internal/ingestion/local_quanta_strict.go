package ingestion

import (
	"sync/atomic"

	"github.com/google/uuid"
)

// LocalQuantaStrict tracks per-campaign strict mode with hysteresis (M8-03).
type LocalQuantaStrict struct {
	enterMicro int64
	exitMicro  int64
	flags      [localQuantaSlotCount]atomic.Uint32
}

// NewLocalQuantaStrict configures strict-band thresholds (enter < exit).
func NewLocalQuantaStrict(enterMicro, exitMicro int64) *LocalQuantaStrict {
	if enterMicro <= 0 {
		enterMicro = 5_000_000
	}
	if exitMicro <= enterMicro {
		exitMicro = enterMicro + 3_000_000
	}
	return &LocalQuantaStrict{
		enterMicro: enterMicro,
		exitMicro:  exitMicro,
	}
}

func (s *LocalQuantaStrict) slotIndex(id uuid.UUID) uint32 {
	return crc32Castagnoli(&id) & localQuantaSlotMask
}

// IsStrict reports whether the campaign must use per-event Lua debits.
func (s *LocalQuantaStrict) IsStrict(id uuid.UUID) bool {
	if s == nil {
		return false
	}
	return s.flags[s.slotIndex(id)].Load() == 1
}

// UpdateFromRedisRemaining applies hysteresis on redis_remaining micro-units.
func (s *LocalQuantaStrict) UpdateFromRedisRemaining(id uuid.UUID, redisRemaining int64) {
	if s == nil {
		return
	}
	idx := s.slotIndex(id)
	if redisRemaining < s.enterMicro {
		s.flags[idx].Store(1)
		return
	}
	if redisRemaining >= s.exitMicro {
		s.flags[idx].Store(0)
	}
}
