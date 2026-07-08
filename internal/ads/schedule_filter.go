package ads

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log/slog"
	"sync/atomic"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// brandCreativeEntry holds one weighted landing URL for a brand.
type brandCreativeEntry struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	Weight int32  `json:"weight"`
}

type brandCreativeMapSnapshot struct {
	byBrand map[uuid.UUID][]brandCreativeEntry
}

func (s *BrandCreativeStore) brandCreativeSnapshot() *brandCreativeMapSnapshot {
	v, ok := s.cache.Load().(*brandCreativeMapSnapshot)
	if !ok || v == nil {
		return &brandCreativeMapSnapshot{}
	}
	return v
}

// BrandCreativeStore caches brand creatives in memory for click landing URL selection.
type BrandCreativeStore struct {
	rdb   redis.UniversalClient
	cache atomic.Value
}

// NewBrandCreativeStore creates an empty in-memory creative cache backed by Redis.
func NewBrandCreativeStore(rdb redis.UniversalClient) *BrandCreativeStore {
	s := &BrandCreativeStore{rdb: rdb}
	s.cache.Store(&brandCreativeMapSnapshot{byBrand: make(map[uuid.UUID][]brandCreativeEntry)})
	return s
}

// LoadFromRedis refreshes one brand's creative list from Redis into the local cache.
func (s *BrandCreativeStore) LoadFromRedis(ctx context.Context, brandID uuid.UUID) {
	if s.rdb == nil {
		return
	}
	raw, err := s.rdb.Get(ctx, "brand:creatives:"+brandID.String()).Bytes()
	if err != nil {
		return
	}
	var entries []brandCreativeEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		brandCreativeReplicaParseErrors.Inc()
		slog.Warn("brand creative replica corrupt", "brand_id", brandID, "error", err)
		return
	}
	current := s.brandCreativeSnapshot().byBrand
	next := make(map[uuid.UUID][]brandCreativeEntry, len(current)+1)
	for k, v := range current {
		next[k] = v
	}
	next[brandID] = entries
	s.cache.Store(&brandCreativeMapSnapshot{byBrand: next})
}

// SelectLandingURL returns a deterministic weighted creative URL for a user.
func (s *BrandCreativeStore) SelectLandingURL(brandID uuid.UUID, userID string) string {
	entries := s.brandCreativeSnapshot().byBrand[brandID]
	if len(entries) == 0 {
		return ""
	}
	if len(entries) == 1 {
		return entries[0].URL
	}

	total := int32(0)
	for _, e := range entries {
		total += e.Weight
	}
	if total <= 0 {
		return entries[0].URL
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	_, _ = h.Write([]byte(brandID.String()))
	bucket := int32(h.Sum32() % uint32(total))

	var acc int32
	for _, e := range entries {
		acc += e.Weight
		if bucket < acc {
			return e.URL
		}
	}
	return entries[len(entries)-1].URL
}

// ScheduleFilter rejects events outside campaign start, end, or daypart windows.
type ScheduleFilter struct {
	registry domain.CampaignRegistry
}

// NewScheduleFilter builds a schedule gate backed by the in-memory campaign registry.
func NewScheduleFilter(registry domain.CampaignRegistry) *ScheduleFilter {
	return &ScheduleFilter{registry: registry}
}

// Check returns ErrScheduleBlocked when the event falls outside delivery hours.
func (f *ScheduleFilter) Check(ctx context.Context, evt *domain.Event) error {
	camp, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}
	now := time.Now()
	if camp.StartAt != nil && now.Before(*camp.StartAt) {
		return ErrScheduleBlocked
	}
	if camp.EndAt != nil && !now.Before(*camp.EndAt) {
		return ErrScheduleBlocked
	}
	if len(camp.DaypartHours) > 0 {
		if camp.Location == nil {
			return ErrScheduleBlocked
		}
		hour := int16(now.In(camp.Location).Hour())
		if _, allowed := camp.DaypartHours[hour]; !allowed {
			return ErrScheduleBlocked
		}
	}
	return nil
}

// DaypartSliceToSet converts daypart hour lists into O(1) lookup sets for the registry.
func DaypartSliceToSet(hours []int16) map[int16]struct{} {
	if len(hours) == 0 {
		return nil
	}
	m := make(map[int16]struct{}, len(hours))
	for _, h := range hours {
		m[h] = struct{}{}
	}
	return m
}
