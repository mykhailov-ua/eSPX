package unit

import (
	"context"
	"testing"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/stretchr/testify/assert"
)

func TestIPRateLimiter(t *testing.T) {
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	limiter := ads.NewIPRateLimiter(rdb, 3, 2*time.Second)

	evt1 := ads.Event{IP: "192.168.1.1"}
	evt2 := ads.Event{IP: "192.168.1.2"}

	// IP 1: Allow up to 3
	assert.NoError(t, limiter.Check(ctx, evt1))
	assert.NoError(t, limiter.Check(ctx, evt1))
	assert.NoError(t, limiter.Check(ctx, evt1))

	// IP 1: 4th should be rejected
	assert.ErrorIs(t, limiter.Check(ctx, evt1), ads.ErrRateLimitExceeded)

	// IP 2: Should still be allowed
	assert.NoError(t, limiter.Check(ctx, evt2))

	// Wait for TTL to expire
	time.Sleep(2500 * time.Millisecond)

	// IP 1: Should be allowed again
	assert.NoError(t, limiter.Check(ctx, evt1))
}

func TestDuplicateEventFilter(t *testing.T) {
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	filter := ads.NewDuplicateEventFilter(rdb, 1*time.Second)

	evt := ads.Event{ClickID: "click_abc_123"}
	evtOther := ads.Event{ClickID: "click_xyz_987"}

	// First time allowed
	assert.NoError(t, filter.Check(ctx, evt))
	assert.NoError(t, filter.Check(ctx, evtOther))

	// Second time rejected
	assert.ErrorIs(t, filter.Check(ctx, evt), ads.ErrDuplicateEvent)

	// Events without ClickID should always pass
	evtEmpty := ads.Event{ClickID: ""}
	assert.NoError(t, filter.Check(ctx, evtEmpty))
	assert.NoError(t, filter.Check(ctx, evtEmpty))

	// Wait for TTL
	time.Sleep(1500 * time.Millisecond)

	// Allowed again
	assert.NoError(t, filter.Check(ctx, evt))
}

func TestFilterEngine(t *testing.T) {
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	limiter := ads.NewIPRateLimiter(rdb, 3, 5*time.Second)
	dupFilter := ads.NewDuplicateEventFilter(rdb, 5*time.Second)

	engine := ads.NewFilterEngine(limiter, dupFilter)

	evt1 := ads.Event{IP: "10.0.0.1", ClickID: "c_1"}
	evt2 := ads.Event{IP: "10.0.0.1", ClickID: "c_2"}
	evt3 := ads.Event{IP: "10.0.0.1", ClickID: "c_3"}

	// Allowed
	assert.NoError(t, engine.Check(ctx, evt1))

	// Rejected by dupFilter (same ClickID)
	assert.ErrorIs(t, engine.Check(ctx, evt1), ads.ErrDuplicateEvent)

	// Allowed (diff ClickID, 2nd request for IP)
	assert.NoError(t, engine.Check(ctx, evt2))

	// Rejected by IP limiter (3rd request for IP)
	assert.ErrorIs(t, engine.Check(ctx, evt3), ads.ErrRateLimitExceeded)
}
