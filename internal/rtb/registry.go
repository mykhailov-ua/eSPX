package rtb

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
)

// AlignedBudget prevents false sharing during concurrent atomic updates.
type AlignedBudget struct {
	Value int64
	_     [7]int64
}

type budgetSlice struct {
	data []AlignedBudget
}

// BudgetStore uses a flat backing array to avoid GC scanning and pointer chasing.
type BudgetStore struct {
	mu      sync.Mutex
	slots   map[uuid.UUID]uint32
	budgets atomic.Pointer[budgetSlice]
}

func NewBudgetStore() *BudgetStore {
	s := &BudgetStore{
		slots: make(map[uuid.UUID]uint32),
	}
	initial := &budgetSlice{
		data: make([]AlignedBudget, 0, 10000),
	}
	s.budgets.Store(initial)
	return s
}

func (s *BudgetStore) GetOrAllocateSlot(id uuid.UUID, initialBudget int64) uint32 {
	s.mu.Lock()
	if idx, exists := s.slots[id]; exists {
		s.mu.Unlock()
		return idx
	}

	currSlice := s.budgets.Load()
	idx := uint32(len(currSlice.data))

	newCap := cap(currSlice.data)
	if len(currSlice.data)+1 > newCap {
		if newCap == 0 {
			newCap = 10000
		} else {
			newCap = newCap * 2
		}
	}

	newData := make([]AlignedBudget, len(currSlice.data)+1, newCap)
	copy(newData, currSlice.data)
	newData[idx] = AlignedBudget{Value: initialBudget}

	s.budgets.Store(&budgetSlice{data: newData})
	s.slots[id] = idx
	s.mu.Unlock()
	return idx
}

func (s *BudgetStore) LoadBudget(idx uint32) int64 {
	slice := s.budgets.Load()
	if idx >= uint32(len(slice.data)) {
		return 0
	}
	return atomic.LoadInt64(&slice.data[idx].Value)
}

// CheckAndSpend limits heap escape by avoiding exposing pointers to caller stack frames.
func (s *BudgetStore) CheckAndSpend(idx uint32, limit int64) bool {
	slice := s.budgets.Load()
	if idx >= uint32(len(slice.data)) {
		return false
	}
	ptr := &slice.data[idx].Value
	for {
		curr := atomic.LoadInt64(ptr)
		if curr < limit {
			return false
		}
		if atomic.CompareAndSwapInt64(ptr, curr, curr-limit) {
			return true
		}
	}
}

func (s *BudgetStore) GetBudget(id uuid.UUID) int64 {
	s.mu.Lock()
	idx, exists := s.slots[id]
	s.mu.Unlock()
	if !exists {
		return 0
	}
	slice := s.budgets.Load()
	return atomic.LoadInt64(&slice.data[idx].Value)
}

func (s *BudgetStore) SetBudget(id uuid.UUID, val int64) {
	s.mu.Lock()
	idx, exists := s.slots[id]
	if !exists {
		currSlice := s.budgets.Load()
		idx = uint32(len(currSlice.data))

		newCap := cap(currSlice.data)
		if len(currSlice.data)+1 > newCap {
			if newCap == 0 {
				newCap = 10000
			} else {
				newCap = newCap * 2
			}
		}

		newData := make([]AlignedBudget, len(currSlice.data)+1, newCap)
		copy(newData, currSlice.data)
		newData[idx] = AlignedBudget{Value: val}

		s.budgets.Store(&budgetSlice{data: newData})
		s.slots[id] = idx
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	slice := s.budgets.Load()
	atomic.StoreInt64(&slice.data[idx].Value, val)
}

// CampaignAuctionRegistry organizes campaign metadata in a Structure of Arrays (SoA) layout.
type CampaignAuctionRegistry struct {
	Count         int
	CampaignIDs   []uuid.UUID
	BidFloors     []int64
	DeviceMasks   []uint8
	CategoryMasks []uint64
	GeoHashes     []uint32
	Weights       []uint32
	BudgetIndices []uint32
}

type CampaignData struct {
	ID           uuid.UUID
	BidFloor     int64
	DeviceMask   uint8
	CategoryMask uint64
	GeoHashVal   uint32
	Weight       uint32
	Budget       int64
}

// PaddedShard isolates partitions to prevent cache line bouncing during pointer swaps.
type PaddedShard struct {
	pointer atomic.Pointer[CampaignAuctionRegistry]
	_       [56]byte
}

type Registry struct {
	shards [16]PaddedShard
	store  *BudgetStore
}

func NewRegistry(store *BudgetStore) *Registry {
	r := &Registry{store: store}
	for i := 0; i < 16; i++ {
		empty := &CampaignAuctionRegistry{}
		r.shards[i].pointer.Store(empty)
	}
	return r
}

func (r *Registry) LoadShard(idx uint32) *CampaignAuctionRegistry {
	return r.shards[idx&15].pointer.Load()
}

func (r *Registry) Store() *BudgetStore {
	return r.store
}

func (r *Registry) UpdateCampaigns(campaigns []CampaignData) {
	var counts [16]int
	for i := range campaigns {
		shardIdx := campaigns[i].GeoHashVal & 15
		counts[shardIdx]++
	}

	var registries [16]*CampaignAuctionRegistry
	for shardIdx := 0; shardIdx < 16; shardIdx++ {
		n := counts[shardIdx]
		registries[shardIdx] = &CampaignAuctionRegistry{
			Count:         n,
			CampaignIDs:   make([]uuid.UUID, n),
			BidFloors:     make([]int64, n),
			DeviceMasks:   make([]uint8, n),
			CategoryMasks: make([]uint64, n),
			GeoHashes:     make([]uint32, n),
			Weights:       make([]uint32, n),
			BudgetIndices: make([]uint32, n),
		}
	}

	var writeIndices [16]int
	for i := range campaigns {
		c := &campaigns[i]
		shardIdx := c.GeoHashVal & 15
		reg := registries[shardIdx]
		wIdx := writeIndices[shardIdx]

		reg.CampaignIDs[wIdx] = c.ID
		reg.BidFloors[wIdx] = c.BidFloor
		reg.DeviceMasks[wIdx] = c.DeviceMask
		reg.CategoryMasks[wIdx] = c.CategoryMask
		reg.GeoHashes[wIdx] = c.GeoHashVal
		reg.Weights[wIdx] = c.Weight
		reg.BudgetIndices[wIdx] = r.store.GetOrAllocateSlot(c.ID, c.Budget)

		writeIndices[shardIdx]++
	}

	for shardIdx := 0; shardIdx < 16; shardIdx++ {
		r.shards[shardIdx].pointer.Store(registries[shardIdx])
	}
}

func (r *Registry) SaveSnapshot(path string) error {
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

	if _, err := w.Write([]byte("ESPRRTBS")); err != nil {
		return err
	}
	if err := writeUint32(w, 1); err != nil {
		return err
	}

	r.store.mu.Lock()
	slotsCount := len(r.store.slots)
	keys := make([]uuid.UUID, 0, slotsCount)
	vals := make([]uint32, 0, slotsCount)
	for k, v := range r.store.slots {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	r.store.mu.Unlock()

	if err := writeUint32(w, uint32(slotsCount)); err != nil {
		return err
	}
	if slotsCount > 0 {
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&keys[0])), slotsCount*16)
		if _, err := w.Write(buf); err != nil {
			return err
		}
		buf = unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), slotsCount*4)
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}

	currSlice := r.store.budgets.Load()
	budgetsData := currSlice.data
	budgetsCount := len(budgetsData)

	if err := writeUint32(w, uint32(budgetsCount)); err != nil {
		return err
	}
	if budgetsCount > 0 {
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&budgetsData[0])), budgetsCount*64)
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	runtime.KeepAlive(currSlice)

	for i := 0; i < 16; i++ {
		shard := r.shards[i].pointer.Load()
		count := 0
		if shard != nil {
			count = shard.Count
		}
		if err := writeUint32(w, uint32(count)); err != nil {
			return err
		}

		if count > 0 {
			if len(shard.CampaignIDs) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.CampaignIDs[0])), count*16)
				if _, err := w.Write(buf); err != nil {
					return err
				}
			}
			if len(shard.BidFloors) > 0 {
				buf := unsafe.Slice((*byte)(unsafe.Pointer(&shard.BidFloors[0])), count*8)
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

func (r *Registry) LoadSnapshot(path string) error {
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
	if string(magic[:]) != "ESPRRTBS" {
		return fmt.Errorf("invalid magic header")
	}

	version, err := readUint32(reader)
	if err != nil {
		return err
	}
	if version != 1 {
		return fmt.Errorf("unsupported snapshot version: %d", version)
	}

	slotsCount, err := readUint32(reader)
	if err != nil {
		return err
	}

	keys := make([]uuid.UUID, slotsCount)
	vals := make([]uint32, slotsCount)
	if slotsCount > 0 {
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&keys[0])), slotsCount*16)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return err
		}
		buf = unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), slotsCount*4)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return err
		}
	}

	slots := make(map[uuid.UUID]uint32, slotsCount)
	for i := uint32(0); i < slotsCount; i++ {
		slots[keys[i]] = vals[i]
	}

	budgetsCount, err := readUint32(reader)
	if err != nil {
		return err
	}

	budgetsData := make([]AlignedBudget, budgetsCount)
	if budgetsCount > 0 {
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&budgetsData[0])), budgetsCount*64)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return err
		}
	}

	var shards [16]*CampaignAuctionRegistry
	for i := 0; i < 16; i++ {
		count, err := readUint32(reader)
		if err != nil {
			return err
		}

		if count > 0 {
			reg := &CampaignAuctionRegistry{
				Count:         int(count),
				CampaignIDs:   make([]uuid.UUID, count),
				BidFloors:     make([]int64, count),
				DeviceMasks:   make([]uint8, count),
				CategoryMasks: make([]uint64, count),
				GeoHashes:     make([]uint32, count),
				Weights:       make([]uint32, count),
				BudgetIndices: make([]uint32, count),
			}

			buf := unsafe.Slice((*byte)(unsafe.Pointer(&reg.CampaignIDs[0])), count*16)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}

			buf = unsafe.Slice((*byte)(unsafe.Pointer(&reg.BidFloors[0])), count*8)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
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

			shards[i] = reg
		} else {
			shards[i] = &CampaignAuctionRegistry{}
		}
	}

	r.store.mu.Lock()
	r.store.slots = slots
	r.store.budgets.Store(&budgetSlice{data: budgetsData})
	r.store.mu.Unlock()

	for i := 0; i < 16; i++ {
		r.shards[i].pointer.Store(shards[i])
	}

	return nil
}

func (r *Registry) StartPersistence(ctx context.Context, path string, interval time.Duration) error {
	if path == "" {
		return nil
	}

	if err := r.LoadSnapshot(path); err != nil {
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
				if err := r.SaveSnapshot(path); err != nil {
					slog.Error("failed to save final registry snapshot", "path", path, "error", err)
				} else {
					slog.Info("final registry snapshot saved successfully")
				}
				return
			case <-ticker.C:
				if err := r.SaveSnapshot(path); err != nil {
					slog.Error("failed to save periodic registry snapshot", "path", path, "error", err)
				}
			}
		}
	}()

	return nil
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
