package ingestion

import (
	"sync/atomic"

	"github.com/google/uuid"
)

const (
	localQuantaCacheLine = 64
	localQuantaSlotCount = 4096
	localQuantaSlotMask  = localQuantaSlotCount - 1
)

// LocalQuantaMode controls whether local quanta debits affect responses.
const (
	LocalQuantaOff    uint32 = 0
	LocalQuantaShadow uint32 = 1
	LocalQuantaLive   uint32 = 2
)

// LocalQuantaCell is a cache-line-isolated campaign quantum counter (M8-01, GUIDE §4).
// One logical pool per campaign_id across pinned workers (M8-08).
type LocalQuantaCell struct {
	campaignHash uint32
	_            uint32
	remaining    atomic.Int64
	chunkSize    int64
	rpsEMA       atomic.Uint64 // fixed-point EMA: value * 1000
	campaignID   uuid.UUID
	_            [localQuantaCacheLine - 8 - 8 - 8 - 8 - 16]byte
}

// LocalQuantaLedger holds campaign-global local budget chunks in SoA layout.
type LocalQuantaLedger struct {
	cells [localQuantaSlotCount]LocalQuantaCell
	mode  atomic.Uint32
}

// NewLocalQuantaLedger returns an empty ledger (mode off until configured).
func NewLocalQuantaLedger() *LocalQuantaLedger {
	return &LocalQuantaLedger{}
}

// SetMode configures off | shadow | live local quanta behavior (M8-06).
func (l *LocalQuantaLedger) SetMode(mode string) {
	switch mode {
	case "shadow":
		l.mode.Store(LocalQuantaShadow)
	case "live":
		l.mode.Store(LocalQuantaLive)
	default:
		l.mode.Store(LocalQuantaOff)
	}
}

// Mode returns the current local quanta mode.
func (l *LocalQuantaLedger) Mode() uint32 {
	return l.mode.Load()
}

func (l *LocalQuantaLedger) cellFor(id uuid.UUID) (*LocalQuantaCell, uint32) {
	h := crc32Castagnoli(&id)
	return &l.cells[h&localQuantaSlotMask], h
}

// HasCredit reports whether the campaign owns a local chunk with any remaining balance.
func (l *LocalQuantaLedger) HasCredit(id uuid.UUID) bool {
	cell, h := l.cellFor(id)
	return cell.campaignHash == h && cell.remaining.Load() > 0
}

// Remaining returns the local micro-units left for a campaign (0 if unassigned).
func (l *LocalQuantaLedger) Remaining(id uuid.UUID) int64 {
	cell, h := l.cellFor(id)
	if cell.campaignHash != h {
		return 0
	}
	return cell.remaining.Load()
}

// ChunkSize returns the active chunk size for a campaign.
func (l *LocalQuantaLedger) ChunkSize(id uuid.UUID) int64 {
	cell, h := l.cellFor(id)
	if cell.campaignHash != h {
		return 0
	}
	return cell.chunkSize
}

// TrySpendLocal debits amountMicro from the campaign-global pool (0 allocs/op hot path).
func (l *LocalQuantaLedger) TrySpendLocal(id uuid.UUID, amountMicro int64) bool {
	if amountMicro <= 0 {
		return true
	}
	cell, h := l.cellFor(id)
	if cell.campaignHash != h {
		return false
	}
	for {
		rem := cell.remaining.Load()
		if rem < amountMicro {
			return false
		}
		if cell.remaining.CompareAndSwap(rem, rem-amountMicro) {
			l.recordSpendEMA(cell)
			return true
		}
	}
}

// Credit adds micro-units from a Redis refill into the campaign slot (cold path).
func (l *LocalQuantaLedger) Credit(id uuid.UUID, amountMicro, chunkSize int64) {
	if amountMicro <= 0 {
		return
	}
	cell, h := l.cellFor(id)
	cell.campaignHash = h
	cell.campaignID = id
	if chunkSize > 0 {
		cell.chunkSize = chunkSize
	}
	cell.remaining.Add(amountMicro)
}

// NeedsRefill reports whether local_remaining/chunk_size < thresholdPct (M8-02).
func (l *LocalQuantaLedger) NeedsRefill(id uuid.UUID, thresholdPct int) bool {
	cell, h := l.cellFor(id)
	if cell.campaignHash != h || cell.chunkSize <= 0 {
		return true
	}
	rem := cell.remaining.Load()
	if rem <= 0 {
		return true
	}
	if thresholdPct <= 0 {
		thresholdPct = 20
	}
	threshold := cell.chunkSize * int64(thresholdPct) / 100
	return rem < threshold
}

// RPSEMA returns the smoothed events-per-second estimate for adaptive quanta (M8-07).
func (l *LocalQuantaLedger) RPSEMA(id uuid.UUID) float64 {
	cell, h := l.cellFor(id)
	if cell.campaignHash != h {
		return 0
	}
	return float64(cell.rpsEMA.Load()) / 1000.0
}

func (l *LocalQuantaLedger) recordSpendEMA(cell *LocalQuantaCell) {
	const alphaMilli = 100 // EMA weight ~0.1 per event at 1kHz sampling
	prev := cell.rpsEMA.Load()
	next := prev + alphaMilli
	if next > 1_000_000 {
		next = 1_000_000
	}
	cell.rpsEMA.Store(next)
}

// AdaptiveChunkSize scales chunk from EMA RPS with floor/ceiling (M8-07).
func AdaptiveChunkSize(emaRPS float64, floorMicro, ceilingMicro, baseChunk int64) int64 {
	if baseChunk <= 0 {
		baseChunk = 5_000_000
	}
	if floorMicro <= 0 {
		floorMicro = 500_000
	}
	if ceilingMicro <= 0 {
		ceilingMicro = 50_000_000
	}
	chunk := baseChunk
	if emaRPS > 0 {
		// ~10ms of spend at observed RPS (micro-units ≈ RPS * 10_000 for $0.01 events).
		scaled := int64(emaRPS * 10_000)
		if scaled > 0 {
			chunk = scaled
		}
	}
	if chunk < floorMicro {
		chunk = floorMicro
	}
	if chunk > ceilingMicro {
		chunk = ceilingMicro
	}
	return chunk
}
