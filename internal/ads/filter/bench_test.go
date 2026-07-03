package filter

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"os"
	"testing"
	"time"

	"espx/internal/ads/catalog"
	"espx/internal/ads/sharding"
	"espx/internal/domain"

	adstest "espx/internal/ads/testutil"
	redis "github.com/redis/go-redis/v9"
)

type errGeoProvider struct{}

func (errGeoProvider) GetCountry(ip string) (string, error) {
	return "", errors.New("geo lookup failed")
}
func (errGeoProvider) IsAnonymous(ip string) (bool, error) { return false, nil }
func (errGeoProvider) Close() error                        { return nil }

// Shared geo filter benchmark setup with configurable provider.
func benchGeoFilterWithCountries(b *testing.B, geo GeoProvider) {
	campID := uuid.New()
	adstest.CachedMockCamp.Store(&domain.Campaign{
		ID:              campID,
		TargetCountries: map[string]struct{}{"US": {}},
	})
	b.Cleanup(func() { adstest.CachedMockCamp.Store(nil) })

	f := NewGeoFilter(geo, &adstest.MockRegistry{})
	evt := &domain.Event{
		IP:         "8.8.8.8",
		CampaignID: campID,
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

// Tracks geo filter cost when lookup returns error.
func BenchmarkGeoFilter_lookupError(b *testing.B) {
	benchGeoFilterWithCountries(b, errGeoProvider{})
}

// Tracks geo filter cost on successful country match.
func BenchmarkGeoFilter_lookupOK(b *testing.B) {
	benchGeoFilterWithCountries(b, &MockGeoProvider{})
}

// Tracks geo filter cost with real MaxMind country lookup.
func BenchmarkGeoFilter_MaxMindCountry(b *testing.B) {
	const path = "deploy/geoip/GeoLite2-Country.mmdb"
	if _, err := os.Stat(path); err != nil {
		b.Skip("GeoLite2-Country.mmdb not present at " + path)
	}
	geo, err := NewMaxMindProvider(path)
	if err != nil {
		b.Fatalf("open mmdb: %v", err)
	}
	b.Cleanup(func() { _ = geo.Close() })
	benchGeoFilterWithCountries(b, geo)
}

// Tracks fraud filter datacenter IP check cost.
func BenchmarkFraudFilter_DC(b *testing.B) {
	geo := &MockGeoProvider{}
	f := NewFraudFilter(geo)
	evt := &domain.Event{
		IP: "1.1.1.66",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

// Tracks geo filter end-to-end check cost.
func BenchmarkGeoFilter(b *testing.B) {
	geo := &MockGeoProvider{}
	registry := &adstest.MockRegistry{}
	f := NewGeoFilter(geo, registry)
	evt := &domain.Event{
		IP:         "1.1.1.1",
		CampaignID: uuid.New(),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

// Tracks IP rate limiter Redis check cost per event.
func BenchmarkIPRateLimiter_Check(b *testing.B) {
	rdb := &adstest.MockRedisClient{}
	l := NewIPRateLimiter(rdb, 100, 10*time.Minute)
	evt := &domain.Event{
		IP: "192.168.1.1",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.Check(ctx, evt)
	}
}

// Tracks duplicate event filter Redis SET NX cost.
func BenchmarkDuplicateEventFilter_Check(b *testing.B) {
	rdb := &adstest.MockRedisClient{}
	f := NewDuplicateEventFilter(rdb, 1*time.Hour)
	evt := &domain.Event{
		Type:    "click",
		ClickID: "click123",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

// Tracks impression timestamp key format allocation cost.
func BenchmarkKeyFormatting_impTSKey(b *testing.B) {
	evt := &domain.Event{
		UserID:     "user123",
		CampaignID: uuid.New(),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := bufPool.Get().(*bufWrapper)
		w.Buf = w.Buf[:0]
		w.Buf = append(w.Buf, "imp_ts:"...)
		w.Buf = append(w.Buf, evt.UserID...)
		w.Buf = append(w.Buf, ':')
		w.Buf = appendUUID(w.Buf, evt.CampaignID)
		key := unsafeString(w.Buf)
		_ = key
		bufPool.Put(w)
	}
}

// Tracks IP rate limiter key format allocation cost.
func BenchmarkKeyFormatting_IPRateLimiter(b *testing.B) {
	evt := &domain.Event{
		IP: "192.168.1.1",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := bufPool.Get().(*bufWrapper)
		w.Buf = w.Buf[:0]
		w.Buf = append(w.Buf, "ratelimit:ip:"...)
		w.Buf = append(w.Buf, evt.IP...)
		key := unsafeString(w.Buf)
		_ = key
		bufPool.Put(w)
	}
}

// Tracks duplicate event key format allocation cost.
func BenchmarkKeyFormatting_DuplicateEventFilter(b *testing.B) {
	evt := &domain.Event{
		Type:    "click",
		ClickID: "click123",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := bufPool.Get().(*bufWrapper)
		w.Buf = w.Buf[:0]
		w.Buf = append(w.Buf, "dup:"...)
		w.Buf = append(w.Buf, evt.Type...)
		w.Buf = append(w.Buf, ':')
		w.Buf = append(w.Buf, evt.ClickID...)
		key := unsafeString(w.Buf)
		_ = key
		bufPool.Put(w)
	}
}

// Tracks unified filter Lua check cost with mock Redis.
func BenchmarkUnifiedFilter_Check(b *testing.B) {
	rdb := &adstest.MockRedisClient{}
	sharder := sharding.NewJumpHashSharder(1)
	registry := &adstest.MockRegistry{}

	f := NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		sharder,
		registry,
		nil,
		100,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10000,
	)

	evt := &domain.Event{
		Type:       "click",
		IP:         "1.1.1.1",
		UserID:     "user123",
		CampaignID: uuid.New(),
		ClickID:    "click123",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

// Tracks Redis budget check-and-spend Lua cost.
func BenchmarkRedisBudgetManager_CheckAndSpend(b *testing.B) {
	rdb := &adstest.MockRedisClient{}
	bm := catalog.NewRedisBudgetManager(rdb, nil, time.Hour)

	ctx := context.Background()
	customerID := uuid.New()
	campaignID := uuid.New()
	clickID := "click123"
	amount := int64(100_000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bm.CheckAndSpend(ctx, customerID, campaignID, clickID, amount)
	}
}
