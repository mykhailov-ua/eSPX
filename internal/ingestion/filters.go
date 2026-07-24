package ingestion

import (
	"context"
	"errors"
	"sync"
	"time"
	"unsafe"

	"espx/internal/campaignmodel"

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
	ErrMigrationFenced        = errors.New("campaign debit fenced")
	ErrLicenseExpired         = errors.New("license expired")
	ErrDailyQuotaExceeded     = errors.New("daily quota exceeded")
	ErrRegistryStale          = errors.New("registry stale: campaign unknown while control plane unreachable")
	ErrShardUnavailable       = errors.New("shard unavailable")
)

// bufWrapper holds a reusable byte buffer for zero-allocation Redis key construction.
type bufWrapper struct {
	buf []byte
}

// bufPool recycles key buffers shared across filter implementations.
var bufPool = sync.Pool{
	New: func() any {
		return &bufWrapper{
			buf: make([]byte, 0, 128),
		}
	},
}

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

func NewFraudFilter(geo GeoProvider) *FraudFilter {
	return &FraudFilter{
		geo: geo,
	}
}

// Check marks anonymous IPs as fraud without blocking on GeoIP lookup failures.
func (f *FraudFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	isAnon, err := f.geo.IsAnonymous(evt.IP)
	if err == nil && isAnon {
		addFraudSignal(evt, FraudReasonDatacenterIP)
	}
	return nil
}

// GeoFilter enforces campaign country targeting without rejecting on transient GeoIP gaps.
type GeoFilter struct {
	geo      GeoProvider
	registry campaignmodel.CampaignRegistry
}

func NewGeoFilter(geo GeoProvider, registry campaignmodel.CampaignRegistry) *GeoFilter {
	return &GeoFilter{
		geo:      geo,
		registry: registry,
	}
}

// Check blocks events whose country is outside the campaign target set.
func (f *GeoFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	start := monotonicNano()
	err := f.checkGeo(evt)
	observeHistogramSampled(&geoMetricsSeq, luaMetricsSampleMask, filterGeoDuration, start)
	return err
}

func (f *GeoFilter) checkGeo(evt *campaignmodel.Event) error {
	camp, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		if reg, ok := f.registry.(*Registry); ok && reg.IsStaleMode() {
			return ErrRegistryStale
		}
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
	manager          campaignmodel.BudgetManager
	registry         campaignmodel.CampaignRegistry
	clickAmount      int64
	impressionAmount int64
}

func NewBudgetFilter(manager campaignmodel.BudgetManager, registry campaignmodel.CampaignRegistry, clickAmount, impressionAmount int64) *BudgetFilter {
	return &BudgetFilter{
		manager:          manager,
		registry:         registry,
		clickAmount:      clickAmount,
		impressionAmount: impressionAmount,
	}
}

// Check spends budget for the event type or returns ErrBudgetExhausted.
func (f *BudgetFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	customerID, ok := f.registry.GetCustomerID(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
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
	Check(ctx context.Context, evt *campaignmodel.Event) error
}

// FilterEngine runs an ordered filter chain under one shared deadline budget.
type FilterEngine struct {
	filters  []EventFilter
	timeout  time.Duration
	registry campaignmodel.CampaignRegistry
	watcher  *SettingsWatcher
}

// NewFilterEngine composes filters with a monotonic deadline enforced between checks.
func NewFilterEngine(timeout time.Duration, filters ...EventFilter) *FilterEngine {
	return &FilterEngine{filters: filters, timeout: timeout}
}

// SetRegistry attaches the campaign catalog used for per-campaign fraud tier mapping.
func (e *FilterEngine) SetRegistry(registry campaignmodel.CampaignRegistry) {
	e.registry = registry
}

// SetSettingsWatcher attaches the settings watcher used for ML score boosts.
func (e *FilterEngine) SetSettingsWatcher(watcher *SettingsWatcher) {
	e.watcher = watcher
}

// Check runs filters in order until one rejects or the deadline expires.
// Production tracker stores the monotonic deadline on evt.FilterDeadlineMono (zero allocs).
func (e *FilterEngine) Check(ctx context.Context, evt *campaignmodel.Event) error {
	if e.timeout > 0 && evt != nil {
		evt.FilterDeadlineMono = monotonicNano() + e.timeout.Nanoseconds()
	}
	acc := attachFraudAccumulator(evt)

	var boost uint8
	if e.watcher != nil && evt != nil {
		boosts := e.watcher.GetFraudScoreBoosts()
		if boosts != nil {
			boost = boosts.Boosts[evt.CampaignID]
		}
	}

	var retErr error
	for _, f := range e.filters {
		if filterDeadlineExceededEvt(evt, ctx) {
			retErr = context.DeadlineExceeded
			break
		}
		if _, ok := f.(*UnifiedFilter); ok && acc.shouldShortCircuitFraudBudget() {
			var camp *campaignmodel.Campaign
			if e.registry != nil && evt != nil {
				camp, _ = e.registry.GetCampaign(evt.CampaignID)
			}
			layer, err := applyFraudLayerDecision(evt, acc, camp, boost)
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
		var camp *campaignmodel.Campaign
		if e.registry != nil && evt != nil {
			camp, _ = e.registry.GetCampaign(evt.CampaignID)
		}
		layer, err := applyFraudLayerDecision(evt, acc, camp, boost)
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

// DuplicateEventFilter rejects replays using a TTL sized for worst-case stream recovery lag.
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

// Check rejects events whose type and click id were seen within the TTL window.
func (f *DuplicateEventFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	if evt.ClickID == "" {
		return nil
	}

	w := bufPool.Get().(*bufWrapper)
	w.buf = w.buf[:0]
	w.buf = append(w.buf, "dup:"...)
	w.buf = append(w.buf, evt.Type...)
	w.buf = append(w.buf, ':')
	w.buf = append(w.buf, evt.ClickID...)
	key := unsafeString(w.buf)

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
	watcher *SettingsWatcher
}

func NewEmergencyBreakerFilter(watcher *SettingsWatcher) *EmergencyBreakerFilter {
	return &EmergencyBreakerFilter{watcher: watcher}
}

// Check returns ErrEmergencyBreakerActive when the breaker is enabled in dynamic config.
func (f *EmergencyBreakerFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	if f.watcher != nil && f.watcher.Get().EmergencyBreaker {
		return ErrEmergencyBreakerActive
	}
	return nil
}

// PlacementBlacklistFilter rejects events from paused placements (subids/zones).
type PlacementBlacklistFilter struct {
	rdbs []redis.UniversalClient
}

func NewPlacementBlacklistFilter(rdbs []redis.UniversalClient) *PlacementBlacklistFilter {
	return &PlacementBlacklistFilter{rdbs: rdbs}
}

// Check rejects the event if the placement_id is in the campaign's placement blacklist.
func (f *PlacementBlacklistFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	if evt == nil || evt.PlacementID == "" {
		return nil
	}
	rdb := pickLocalGlobalShard(f.rdbs)
	if rdb == nil {
		return nil
	}

	w := bufPool.Get().(*bufWrapper)
	w.buf = appendCampaignHashTag(w.buf[:0], evt.CampaignID)
	w.buf = append(w.buf, "blacklist:placement:"...)
	w.buf = appendUUID(w.buf, evt.CampaignID)
	key := unsafeString(w.buf)

	isBlacklisted, err := rdb.HExists(ctx, key, evt.PlacementID).Result()
	bufPool.Put(w)
	if err != nil {
		return nil // Fail open
	}
	if isBlacklisted {
		return ErrPlacementBlocked
	}
	return nil
}

var ErrPlacementBlocked = errors.New("placement blocked")
