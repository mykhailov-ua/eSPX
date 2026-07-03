package filter

import (
	"context"
	"errors"
	"sync"
	"time"
	"unsafe"

	"espx/internal/ads/catalog"
	"espx/internal/domain"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// Filter rejection errors returned to track handlers and metrics classifiers.
var (
	ErrRateLimitExceeded      = errors.New("rate limit exceeded")
	ErrDuplicateEvent         = errors.New("duplicate event detected")
	ErrBudgetExhausted        = errors.New("budget exhausted")
	ErrCampaignNotFound       = errors.New("campaign not found in registry")
	ErrPacingExhausted        = errors.New("pacing exhausted")
	ErrFreqLimitExceeded      = errors.New("frequency limit exceeded")
	ErrGeoBlocked             = errors.New("geo-targeting blocked")
	ErrScheduleBlocked        = errors.New("outside delivery schedule")
	ErrFraudDetected          = errors.New("fraud detected")
	ErrEmergencyBreakerActive = errors.New("service temporarily unavailable (emergency breaker active)")
	ErrBidFloorNotMet         = errors.New("bid floor not met")
)

// BufWrapper holds a reusable byte buffer for zero-allocation Redis key construction.
type BufWrapper struct {
	Buf []byte
}

// bufWrapper is kept for internal filter code during migration.
type bufWrapper = BufWrapper

// bufPool recycles key buffers shared across filter implementations.
var bufPool = sync.Pool{
	New: func() any {
		return &BufWrapper{
			Buf: make([]byte, 0, 128),
		}
	},
}

// BufPool recycles key buffers shared across ingest and filter paths.
var BufPool = bufPool

// hexChars is the lookup table for allocation-free UUID string formatting.
const hexChars = "0123456789abcdef"

// appendUUID writes canonical UUID text into reusable buffers to avoid fmt on the filter hot path.
func appendUUID(dst []byte, u uuid.UUID) []byte {
	return append(dst,
		hexChars[u[0]>>4], hexChars[u[0]&0xf],
		hexChars[u[1]>>4], hexChars[u[1]&0xf],
		hexChars[u[2]>>4], hexChars[u[2]&0xf],
		hexChars[u[3]>>4], hexChars[u[3]&0xf],
		'-',
		hexChars[u[4]>>4], hexChars[u[4]&0xf],
		hexChars[u[5]>>4], hexChars[u[5]&0xf],
		'-',
		hexChars[u[6]>>4], hexChars[u[6]&0xf],
		hexChars[u[7]>>4], hexChars[u[7]&0xf],
		'-',
		hexChars[u[8]>>4], hexChars[u[8]&0xf],
		hexChars[u[9]>>4], hexChars[u[9]&0xf],
		'-',
		hexChars[u[10]>>4], hexChars[u[10]&0xf],
		hexChars[u[11]>>4], hexChars[u[11]&0xf],
		hexChars[u[12]>>4], hexChars[u[12]&0xf],
		hexChars[u[13]>>4], hexChars[u[13]&0xf],
		hexChars[u[14]>>4], hexChars[u[14]&0xf],
		hexChars[u[15]>>4], hexChars[u[15]&0xf],
	)
}

// unsafeString views a byte slice as a string without copying when lifetime is bounded.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// FraudFilter flags datacenter and proxy IPs before events enter billing paths.
type FraudFilter struct {
	geo GeoProvider
}

// NewFraudFilter builds an IP anonymity gate backed by a GeoProvider.
func NewFraudFilter(geo GeoProvider) *FraudFilter {
	return &FraudFilter{
		geo: geo,
	}
}

// Check marks anonymous IPs as fraud without blocking on GeoIP lookup failures.
func (f *FraudFilter) Check(ctx context.Context, evt *domain.Event) error {
	isAnon, err := f.geo.IsAnonymous(evt.IP)
	if err == nil && isAnon {
		addFraudSignal(evt, FraudReasonDatacenterIP)
	}
	return nil
}

// GeoFilter enforces campaign country targeting without rejecting on transient GeoIP gaps.
type GeoFilter struct {
	geo      GeoProvider
	registry domain.CampaignRegistry
}

// NewGeoFilter builds a country gate using registry target lists and GeoIP lookups.
func NewGeoFilter(geo GeoProvider, registry domain.CampaignRegistry) *GeoFilter {
	return &GeoFilter{
		geo:      geo,
		registry: registry,
	}
}

// Check blocks events whose country is outside the campaign target set.
func (f *GeoFilter) Check(ctx context.Context, evt *domain.Event) error {
	start := monotonicNano()
	defer observeHistogramSampled(&geoMetricsSeq, luaMetricsSampleMask, filterGeoDuration, start)

	camp, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}

	if len(camp.TargetCountries) == 0 {
		return nil
	}

	var country string
	if evt.IngestGeoResolved {
		country = evt.GeoCountry
	} else {
		var err error
		country, err = f.geo.GetCountry(evt.IP)
		if err != nil {
			filterGeoLookupErrors.Inc()
			return nil
		}
	}
	if country == "" {
		filterGeoLookupErrors.Inc()
		return nil
	}

	if _, allowed := camp.TargetCountries[country]; allowed {
		return nil
	}

	return ErrGeoBlocked
}

// BudgetFilter delegates spend checks to the configured budget manager.
type BudgetFilter struct {
	manager          domain.BudgetManager
	registry         domain.CampaignRegistry
	clickAmount      int64
	impressionAmount int64
}

// NewBudgetFilter creates a per-event spend gate with type-specific charge amounts.
func NewBudgetFilter(manager domain.BudgetManager, registry domain.CampaignRegistry, clickAmount, impressionAmount int64) *BudgetFilter {
	return &BudgetFilter{
		manager:          manager,
		registry:         registry,
		clickAmount:      clickAmount,
		impressionAmount: impressionAmount,
	}
}

// Check spends budget for the event type or returns ErrBudgetExhausted.
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

// EventFilter is the interface for composable pre-stream event gates.
type EventFilter interface {
	Check(ctx context.Context, evt *domain.Event) error
}

// FilterEngine runs an ordered filter chain under one shared deadline budget.
type FilterEngine struct {
	filters  []EventFilter
	timeout  time.Duration
	registry domain.CampaignRegistry
}

// NewFilterEngine composes filters with a monotonic deadline enforced between checks.
func NewFilterEngine(timeout time.Duration, filters ...EventFilter) *FilterEngine {
	return &FilterEngine{filters: filters, timeout: timeout}
}

// SetRegistry attaches the campaign catalog used for per-campaign fraud tier mapping.
func (e *FilterEngine) SetRegistry(registry domain.CampaignRegistry) {
	e.registry = registry
}

// Check runs filters in order until one rejects or the deadline expires.
// Production tracker stores the monotonic deadline on evt.FilterDeadlineMono (zero allocs).
func (e *FilterEngine) Check(ctx context.Context, evt *domain.Event) error {
	if e.timeout > 0 && evt != nil {
		evt.FilterDeadlineMono = monotonicNano() + e.timeout.Nanoseconds()
	}
	acc := attachFraudAccumulator(evt)

	var retErr error
	for _, f := range e.filters {
		if filterDeadlineExceededEvt(evt, ctx) {
			retErr = context.DeadlineExceeded
			break
		}
		if _, ok := f.(*UnifiedFilter); ok && acc.shouldShortCircuitFraudBudget() {
			var camp *domain.Campaign
			if e.registry != nil && evt != nil {
				camp, _ = e.registry.GetCampaign(evt.CampaignID)
			}
			layer, err := applyFraudLayerDecision(evt, acc, camp)
			if err != nil {
				retErr = err
				break
			}
			if layer == FraudLayerL1Reject {
				retErr = ErrFraudDetected
				break
			}
			if layer == FraudLayerL2Shadow {
				break
			}
			continue
		}
		if err := f.Check(ctx, evt); err != nil {
			retErr = err
			break
		}
	}

	if retErr == nil {
		var camp *domain.Campaign
		if e.registry != nil && evt != nil {
			camp, _ = e.registry.GetCampaign(evt.CampaignID)
		}
		layer, err := applyFraudLayerDecision(evt, acc, camp)
		if err != nil {
			retErr = err
		} else if layer == FraudLayerL1Reject {
			retErr = ErrFraudDetected
		}
	}

	if evt != nil && evt.FilterDeadlineMono > 0 {
		evt.FilterDeadlineMono = 0
	}
	releaseFraudAccumulator(evt, acc)
	return retErr
}

// IPRateLimiter caps per-IP event rates to mitigate abuse on the track endpoint.
type IPRateLimiter struct {
	rdb       redis.UniversalClient
	limit     int
	window    time.Duration
	limitAny  any
	windowAny any
}

// NewIPRateLimiter creates a Redis-backed sliding window limiter for client IPs.
func NewIPRateLimiter(rdb redis.UniversalClient, limit int, window time.Duration) *IPRateLimiter {
	return &IPRateLimiter{
		rdb:       rdb,
		limit:     limit,
		window:    window,
		limitAny:  limit,
		windowAny: int64(window.Milliseconds()),
	}
}

// Check increments the IP counter and rejects when the window limit is exceeded.
func (l *IPRateLimiter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.IP == "" {
		return nil
	}

	w := bufPool.Get().(*bufWrapper)
	w.Buf = w.Buf[:0]
	w.Buf = append(w.Buf, "ratelimit:ip:"...)
	w.Buf = append(w.Buf, evt.IP...)
	key := unsafeString(w.Buf)

	pipe := l.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Do(ctx, "PEXPIRE", key, l.windowAny, "NX")
	_, err := pipe.Exec(ctx)
	bufPool.Put(w)

	if err != nil {
		return err
	}

	if incr.Val() > int64(l.limit) {
		return ErrRateLimitExceeded
	}

	return nil
}

// DuplicateEventFilter rejects replays using a TTL sized for worst-case stream recovery lag.
type DuplicateEventFilter struct {
	rdb redis.Cmdable
	ttl time.Duration
}

// NewDuplicateEventFilter creates a Redis SETNX deduplicator for click and event type pairs.
func NewDuplicateEventFilter(rdb redis.Cmdable, ttl time.Duration) *DuplicateEventFilter {
	return &DuplicateEventFilter{
		rdb: rdb,
		ttl: ttl,
	}
}

// Check rejects events whose type and click id were seen within the TTL window.
func (f *DuplicateEventFilter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.ClickID == "" {
		return nil
	}

	w := bufPool.Get().(*bufWrapper)
	w.Buf = w.Buf[:0]
	w.Buf = append(w.Buf, "dup:"...)
	w.Buf = append(w.Buf, evt.Type...)
	w.Buf = append(w.Buf, ':')
	w.Buf = append(w.Buf, evt.ClickID...)
	key := unsafeString(w.Buf)

	ok, err := f.rdb.SetNX(ctx, key, "1", f.ttl).Result()
	bufPool.Put(w)

	if err != nil {
		return err
	}

	if !ok {
		return ErrDuplicateEvent
	}

	return nil
}

// EmergencyBreakerFilter stops ingestion when operators enable the global breaker flag.
type EmergencyBreakerFilter struct {
	watcher *catalog.SettingsWatcher
}

// NewEmergencyBreakerFilter gates events on the dynamic emergency breaker setting.
func NewEmergencyBreakerFilter(watcher *catalog.SettingsWatcher) *EmergencyBreakerFilter {
	return &EmergencyBreakerFilter{watcher: watcher}
}

// Check returns ErrEmergencyBreakerActive when the breaker is enabled in dynamic config.
func (f *EmergencyBreakerFilter) Check(ctx context.Context, evt *domain.Event) error {
	if f.watcher != nil && f.watcher.Get().EmergencyBreaker {
		return ErrEmergencyBreakerActive
	}
	return nil
}
