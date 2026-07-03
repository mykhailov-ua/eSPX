package rtb

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	snapshotMagic         = "ESPRRTBS"
	snapshotVersion       = 4
	snapshotVersionP1     = 3
	snapshotVersionLegacy = 2
	campaignIDWireSize    = 8
	maxSnapshotRetries    = 8
	alignedBudgetStride   = 64
)

type snapshotCapture struct {
	gen         uint64
	slotsCount  int
	keys        []CampaignID
	vals        []uint32
	budgetsCopy []AlignedBudget
	snap        *catalogSnapshot
}

// SaveSnapshot writes the in-memory registry to disk so process restarts can recover budgets and campaign targeting.
func (registry *Registry) SaveSnapshot(path string) error {
	for range maxSnapshotRetries {
		captured := registry.captureSnapshot()
		if captured.stale(registry) {
			continue
		}
		if err := writeSnapshotFile(path, captured); err != nil {
			return err
		}
		if !captured.stale(registry) {
			return nil
		}
	}
	return fmt.Errorf("%w after %d retries", ErrSnapshotUnstable, maxSnapshotRetries)
}

// LoadSnapshot restores registry and budget state from disk after a restart or crash.
func (registry *Registry) LoadSnapshot(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	var magic [8]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil {
		return err
	}
	if string(magic[:]) != snapshotMagic {
		return fmt.Errorf("rtb: invalid snapshot magic")
	}

	version, err := readUint32(reader)
	if err != nil {
		return err
	}
	if version != snapshotVersion && version != snapshotVersionP1 && version != snapshotVersionLegacy {
		return fmt.Errorf("rtb: unsupported snapshot version %d", version)
	}

	slotsCount, err := readUint32(reader)
	if err != nil {
		return err
	}

	keys := make([]CampaignID, slotsCount)
	vals := make([]uint32, slotsCount)
	if slotsCount > 0 {
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&keys[0])), slotsCount*campaignIDWireSize)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return err
		}
		buf = unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), slotsCount*4)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return err
		}
	}

	slots := make(map[CampaignID]uint32, slotsCount)
	for i := uint32(0); i < slotsCount; i++ {
		slots[keys[i]] = vals[i]
	}

	budgetsCount, err := readUint32(reader)
	if err != nil {
		return err
	}

	budgetsData := make([]AlignedBudget, budgetsCount)
	if budgetsCount > 0 {
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&budgetsData[0])), budgetsCount*alignedBudgetStride)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return err
		}
	}

	var shards [geoShardCount]*CampaignAuctionRegistry
	shardLimit := geoShardCount
	if version == snapshotVersionLegacy {
		shardLimit = legacyGeoShardCount
	}
	for i := 0; i < shardLimit; i++ {
		count, err := readUint32(reader)
		if err != nil {
			return err
		}

		if count > 0 {
			reg := &CampaignAuctionRegistry{
				Count:                 int(count),
				CampaignIDs:           make([]CampaignID, count),
				Bids:                  make([]int64, count),
				CTRPPM:                make([]uint32, count),
				Reserves:              make([]int64, count),
				DailyBudgets:          make([]int64, count),
				PacingOpen:            make([]uint8, count),
				DeviceMasks:           make([]uint8, count),
				CategoryMasks:         make([]uint64, count),
				GeoHashes:             make([]uint32, count),
				Weights:               make([]uint32, count),
				BudgetIndices:         make([]uint32, count),
				CustomerBudgetIndices: make([]uint32, count),
			}

			buf := unsafe.Slice((*byte)(unsafe.Pointer(&reg.CampaignIDs[0])), count*campaignIDWireSize)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}

			buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.Bids[0])), count*8)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}

			if version >= snapshotVersionP1 {
				buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.CTRPPM[0])), count*4)
				if _, err := io.ReadFull(reader, buf); err != nil {
					return err
				}
				buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.Reserves[0])), count*8)
				if _, err := io.ReadFull(reader, buf); err != nil {
					return err
				}
			} else {
				for j := uint32(0); j < count; j++ {
					reg.CTRPPM[j] = CTRPPMUnit
				}
			}

			if version >= snapshotVersion {
				buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.DailyBudgets[0])), count*8)
				if _, err := io.ReadFull(reader, buf); err != nil {
					return err
				}
				if _, err := io.ReadFull(reader, reg.PacingOpen); err != nil {
					return err
				}
				buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.CustomerBudgetIndices[0])), count*4)
				if _, err := io.ReadFull(reader, buf); err != nil {
					return err
				}
			} else {
				for j := uint32(0); j < count; j++ {
					reg.PacingOpen[j] = PacingOpen
					reg.CustomerBudgetIndices[j] = invalidCustomerBudgetIdx
				}
			}

			if _, err := io.ReadFull(reader, reg.DeviceMasks); err != nil {
				return err
			}

			buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.CategoryMasks[0])), count*8)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}

			buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.GeoHashes[0])), count*4)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}

			buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.Weights[0])), count*4)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}

			buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.BudgetIndices[0])), count*4)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}

			buildGeoIndex(reg)
			shards[i] = reg
		} else {
			shards[i] = &CampaignAuctionRegistry{}
		}
	}
	for i := shardLimit; i < geoShardCount; i++ {
		shards[i] = &CampaignAuctionRegistry{}
	}

	registry.store.mu.Lock()
	registry.store.slots = slots
	registry.store.budgets.Store(&budgetSlice{data: budgetsData})
	registry.store.mu.Unlock()

	registry.publishCatalog(shards)

	return nil
}

// StartPersistence reloads state on boot and keeps periodic snapshots so budget drift survives restarts.
func (registry *Registry) StartPersistence(ctx context.Context, path string, interval time.Duration) error {
	if path == "" {
		return nil
	}

	if err := registry.LoadSnapshot(path); err != nil {
		if os.IsNotExist(err) {
			slog.Info("no registry snapshot found on startup, starting fresh", "path", path)
		} else {
			slog.Error("failed to load registry snapshot on startup", "path", path, "error", err)
		}
	} else {
		slog.Info("successfully restored campaign registry snapshot", "path", path)
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				slog.Info("shutting down registry persistence: saving final snapshot", "path", path)
				if err := registry.SaveSnapshot(path); err != nil {
					slog.Error("failed to save final registry snapshot", "path", path, "error", err)
				} else {
					slog.Info("final registry snapshot saved successfully")
				}
				return
			case <-ticker.C:
				if err := registry.SaveSnapshot(path); err != nil {
					slog.Error("failed to save periodic registry snapshot", "path", path, "error", err)
				}
			}
		}
	}()

	return nil
}

func (registry *Registry) captureSnapshot() snapshotCapture {
	registry.store.mu.Lock()
	defer registry.store.mu.Unlock()

	captured := snapshotCapture{
		gen:        registry.snapGen.Load(),
		slotsCount: len(registry.store.slots),
		keys:       make([]CampaignID, 0, len(registry.store.slots)),
		vals:       make([]uint32, 0, len(registry.store.slots)),
		snap:       registry.catalog.Load(),
	}
	for k, v := range registry.store.slots {
		captured.keys = append(captured.keys, k)
		captured.vals = append(captured.vals, v)
	}
	currSlice := registry.store.budgets.Load()
	captured.budgetsCopy = make([]AlignedBudget, len(currSlice.data))
	for i := range currSlice.data {
		captured.budgetsCopy[i].Value = atomic.LoadInt64(&currSlice.data[i].Value)
	}
	return captured
}

func (c snapshotCapture) stale(registry *Registry) bool {
	return registry.snapGen.Load() != c.gen
}

func writeSnapshotFile(path string, captured snapshotCapture) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}()

	w := bufio.NewWriter(f)

	if _, err := w.Write([]byte(snapshotMagic)); err != nil {
		return err
	}
	if err := writeUint32(w, snapshotVersion); err != nil {
		return err
	}

	slotsCount := captured.slotsCount
	keys := captured.keys
	vals := captured.vals
	budgetsCopy := captured.budgetsCopy
	snap := captured.snap

	if err := writeUint32(w, uint32(slotsCount)); err != nil {
		return err
	}
	if slotsCount > 0 {
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&keys[0])), slotsCount*campaignIDWireSize)
		if _, err := w.Write(buf); err != nil {
			return err
		}
		buf = unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), slotsCount*4)
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}

	budgetsCount := len(budgetsCopy)
	if err := writeUint32(w, uint32(budgetsCount)); err != nil {
		return err
	}
	if budgetsCount > 0 {
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&budgetsCopy[0])), budgetsCount*alignedBudgetStride)
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	runtime.KeepAlive(budgetsCopy)

	if snap == nil {
		snap = &catalogSnapshot{}
	}
	for i := 0; i < geoShardCount; i++ {
		shard := snap.shards[i]
		count := 0
		if shard != nil {
			count = shard.Count
		}
		if err := writeUint32(w, uint32(count)); err != nil {
			return err
		}

		if count > 0 {
			if len(shard.CampaignIDs) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.CampaignIDs[0])), count*campaignIDWireSize)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.Bids) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.Bids[0])), count*8)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.CTRPPM) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.CTRPPM[0])), count*4)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.Reserves) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.Reserves[0])), count*8)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.DailyBudgets) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.DailyBudgets[0])), count*8)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.PacingOpen) > 0 {
				if _, err := w.Write(shard.PacingOpen); err != nil {
					return err
				}
			}
			if len(shard.CustomerBudgetIndices) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.CustomerBudgetIndices[0])), count*4)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.DeviceMasks) > 0 {
				if _, err := w.Write(shard.DeviceMasks); err != nil {
					return err
				}
			}
			if len(shard.CategoryMasks) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.CategoryMasks[0])), count*8)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.GeoHashes) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.GeoHashes[0])), count*4)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.Weights) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.Weights[0])), count*4)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.BudgetIndices) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.BudgetIndices[0])), count*4)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			runtime.KeepAlive(shard)
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func writeUint32(w *bufio.Writer, val uint32) error {
	if err := w.WriteByte(byte(val)); err != nil {
		return err
	}
	if err := w.WriteByte(byte(val >> 8)); err != nil {
		return err
	}
	if err := w.WriteByte(byte(val >> 16)); err != nil {
		return err
	}
	return w.WriteByte(byte(val >> 24))
}

func readUint32(r *bufio.Reader) (uint32, error) {
	b1, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b2, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b3, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b4, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint32(b1) | (uint32(b2) << 8) | (uint32(b3) << 16) | (uint32(b4) << 24), nil
}
