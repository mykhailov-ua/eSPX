package ads

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

var builderPool = sync.Pool{
	New: func() any {
		return &strings.Builder{}
	},
}

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
	ErrDuplicateEvent    = errors.New("duplicate event detected")
	ErrBudgetExhausted   = errors.New("budget exhausted")
)

type BudgetFilter struct {
	manager          domain.BudgetManager
	registry         domain.CampaignRegistry
	clickAmount      float64
	impressionAmount float64
}

func NewBudgetFilter(manager domain.BudgetManager, registry domain.CampaignRegistry, clickAmount, impressionAmount float64) *BudgetFilter {
	return &BudgetFilter{
		manager:          manager,
		registry:         registry,
		clickAmount:      clickAmount,
		impressionAmount: impressionAmount,
	}
}

func (f *BudgetFilter) Check(ctx context.Context, evt *domain.Event) error {
	customerID, ok := f.registry.GetCustomerID(evt.CampaignID)
	if !ok {
		return errors.New("campaign not found in registry")
	}

	amount := f.clickAmount
	if evt.Type == "impression" {
		amount = f.impressionAmount
	}

	allowed, err := f.manager.CheckAndSpend(ctx, customerID, evt.CampaignID, evt.ClickID, amount)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrBudgetExhausted
	}
	return nil
}

type EventFilter interface {
	Check(ctx context.Context, evt *domain.Event) error
}

type FilterEngine struct {
	filters []EventFilter
}

func NewFilterEngine(filters ...EventFilter) *FilterEngine {
	return &FilterEngine{filters: filters}
}

func (e *FilterEngine) Check(ctx context.Context, evt *domain.Event) error {
	for _, f := range e.filters {
		if err := f.Check(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}

type IPRateLimiter struct {
	rdb    redis.Cmdable
	limit  int
	window time.Duration
}

func NewIPRateLimiter(rdb redis.Cmdable, limit int, window time.Duration) *IPRateLimiter {
	return &IPRateLimiter{
		rdb:    rdb,
		limit:  limit,
		window: window,
	}
}

const rateLimitScript = `
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
if current > tonumber(ARGV[2]) then
    return 1 -- limit exceeded
end
return 0 -- allowed
`

func (l *IPRateLimiter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.IP == "" {
		return nil
	}

	sb := builderPool.Get().(*strings.Builder)
	sb.Reset()
	sb.Grow(13 + len(evt.IP))
	sb.WriteString("ratelimit:ip:")
	sb.WriteString(evt.IP)
	key := sb.String()
	builderPool.Put(sb)

	windowMs := int64(l.window.Milliseconds())

	res, err := l.rdb.Eval(ctx, rateLimitScript, []string{key}, windowMs, l.limit).Result()
	if err != nil {
		// Fail open on Redis error to not disrupt tracking if Redis is slow/down briefly
		return nil
	}

	if res.(int64) == 1 {
		return ErrRateLimitExceeded
	}

	return nil
}

// DuplicateEventFilter prevents processing of events with the same ClickID
// within a short timeframe. This acts as a first-line defense against double clicks
// before the database ON CONFLICT kicks in, saving processing resources.
type DuplicateEventFilter struct {
	rdb redis.Cmdable
	ttl time.Duration
}

func NewDuplicateEventFilter(rdb redis.Cmdable, ttl time.Duration) *DuplicateEventFilter {
	return &DuplicateEventFilter{
		rdb: rdb,
		ttl: ttl,
	}
}

func (f *DuplicateEventFilter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.ClickID == "" {
		return nil
	}

	sb := builderPool.Get().(*strings.Builder)
	sb.Reset()
	sb.Grow(5 + len(evt.Type) + len(evt.ClickID))
	sb.WriteString("dup:")
	sb.WriteString(evt.Type)
	sb.WriteByte(':')
	sb.WriteString(evt.ClickID)
	key := sb.String()
	builderPool.Put(sb)

	ok, err := f.rdb.SetNX(ctx, key, "1", f.ttl).Result()
	if err != nil {
		// Fail open
		return nil
	}

	if !ok {
		return ErrDuplicateEvent
	}

	return nil
}
