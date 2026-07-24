package ingestion

import (
	"context"
	_ "embed"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"espx/internal/campaignmodel"
	"espx/internal/database"
	"espx/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	redis "github.com/redis/go-redis/v9"
)

// unifiedFilterLua holds the Redis script that enforces budget, pacing, dedup, and stream enqueue in one round trip.
//
//go:embed unified-filter.lua
var unifiedFilterLua string

var unifiedFilterLuaAny any

// StringVal wraps a string for zero-copy Redis binary marshaling in Lua args.
type StringVal struct {
	s string
}

// MarshalBinary exposes the wrapped string bytes to go-redis without copying.
func (sv *StringVal) MarshalBinary() ([]byte, error) {
	if len(sv.s) == 0 {
		return nil, nil
	}
	return unsafe.Slice(unsafe.StringData(sv.s), len(sv.s)), nil
}

// UnifiedStringWrappers groups pooled string adapters passed as Lua arguments.
type UnifiedStringWrappers struct {
	clickID     StringVal
	evtType     StringVal
	payload     StringVal
	ip          StringVal
	ua          StringVal
	userID      StringVal
	placementID StringVal
}

var (
	dirtyCampaignsKeyVal = StringVal{s: "budget:dirty_campaigns"}
	dirtyCustomersKeyVal = StringVal{s: "budget:dirty_customers"}
	refillNeededKeyVal   = StringVal{s: "budget:refill_needed"}
	fcapIgnoredKeyVal    = StringVal{s: "fcap:ignored"}
)

// unifiedCheckScratch holds pooled buffers for one UnifiedFilter.Check without defer.
type unifiedCheckScratch struct {
	wDup, wIdem, wDate, wDS, wFcap, wImpTS, wQuota, wRefillLock, wFence, wFrozen bufWrapper
	wDeadlineMono, wNowMono                                                      bufWrapper
	deadlineMonoStr, nowMonoStr                                                  StringVal
	precheck                                                                     luaPrecheckScratch
	args                                                                         []any
	wrappers                                                                     UnifiedStringWrappers
	keyVals                                                                      [unifiedFilterKeyCount]StringVal
	keyArgs                                                                      [unifiedFilterKeyCount]any
}

var unifiedScratchPool = sync.Pool{
	New: func() any {
		s := &unifiedCheckScratch{
			args: make([]any, 35),
		}
		s.wDup.buf = make([]byte, 0, 128)
		s.wIdem.buf = make([]byte, 0, 128)
		s.wDate.buf = make([]byte, 0, 128)
		s.wDS.buf = make([]byte, 0, 128)
		s.wFcap.buf = make([]byte, 0, 128)
		s.wImpTS.buf = make([]byte, 0, 128)
		s.wQuota.buf = make([]byte, 0, 128)
		s.wRefillLock.buf = make([]byte, 0, 128)
		s.wFence.buf = make([]byte, 0, 128)
		s.wFrozen.buf = make([]byte, 0, 128)
		s.wDeadlineMono.buf = make([]byte, 0, 24)
		s.wNowMono.buf = make([]byte, 0, 24)
		for i := range s.keyVals {
			s.keyArgs[i] = &s.keyVals[i]
		}
		return s
	},
}

func (s *unifiedCheckScratch) acquire() {}
func (s *unifiedCheckScratch) release() {}

// appendDate writes pacing date keys without time.Format allocations in unified filter Lua setup.
func appendDate(dst []byte, t time.Time) []byte {
	year, month, day := t.Date()
	return append(dst,
		byte('0'+year/1000),
		byte('0'+(year/100)%10),
		byte('0'+(year/10)%10),
		byte('0'+year%10),
		byte('0'+int(month)/10),
		byte('0'+int(month)%10),
		byte('0'+day/10),
		byte('0'+day%10),
	)
}

// zeroAny and oneAny are reused Lua numeric flag arguments.
var (
	zeroAny any = 0
	oneAny  any = 1
)

// hourAnyCache pre-boxes hour integers passed to the unified filter Lua script.
var hourAnyCache [25]any

// init fills hourAnyCache so Lua args avoid per-request boxing allocations.
func init() {
	unifiedFilterLuaAny = unifiedFilterLua
	budgetFastLuaAny = budgetFastLua
	for i := 0; i <= 24; i++ {
		hourAnyCache[i] = i
	}
	for i := range maxRPDAnyCache {
		maxRPDAnyCache[i] = uint64(i)
	}
}

// DBHealthChecker supports SLA sentinel latency probes against Postgres.
type DBHealthChecker interface {
	Ping(ctx context.Context) error
}

// UnifiedFilter runs budget, pacing, dedup, and stream enqueue in one Redis Lua round trip.
type UnifiedFilter struct {
	rdbs                     []redis.UniversalClient
	sharder                  Sharder
	script                   *redis.Script
	scriptHash               string
	scriptHashAny            any
	registry                 campaignmodel.CampaignRegistry
	repo                     campaignmodel.CampaignRepository
	geo                      GeoProvider
	geoFloors                sync.Map
	rateLimit                int
	rateLimitWindow          time.Duration
	dupTTL                   time.Duration
	idempotencyTTL           time.Duration
	clickAmountMicro         int64
	impressionAmountMicro    int64
	streamName               string
	streamKeyVal             StringVal
	maxStreamLen             int
	rateLimitWindowAny       any
	rateLimitAny             any
	dupTTLAny                any
	idempotencyTTLAny        any
	maxStreamLenAny          any
	clickAmountMicroAny      any
	impressionAmountMicroAny any

	dbHealth               DBHealthChecker
	slaPenaltyActive       atomic.Bool
	p95ThresholdMs         float64
	recoveryEmaMs          float64
	recoveryStableDuration time.Duration
	emaAlpha               float64
	latencySamples         []float64
	latencyIdx             int
	latencyMu              sync.Mutex
	recoveryStartTime      time.Time
	currentEma             float64

	clickAmountMicroHalfAny      any
	impressionAmountMicroHalfAny any
	ttcMinMsAny                  any
	impTsTTLAny                  any
	ttcFailClosedAny             any
	skipBudgetDebitAny           any
	quotaEnabledAny              any
	quotaChunkSizeAny            any
	quotaRefillThresholdPctAny   any
	quotaMode                    string
	localQuotaMode               string
	localQuotaCache              *LocalQuotaCache
	localQuantaLedger            *LocalQuantaLedger
	localQuantaStrict            *LocalQuantaStrict
	localQuantaRefill            *QuotaRefillWorker
	localQuantaPublisher         *BudgetDeltaPublisher
	dbLookupTimeout              time.Duration
	pgFallbackAllowed            bool
	luaMetricsSeq                atomic.Uint64
	fastScript                   *redis.Script
	fastScriptHashAny            any
	fastPathEnabled              atomic.Bool

	luaDurationObservers     []prometheus.Observer
	luaFastDurationObservers []prometheus.Observer
	luaFastPathCounters      []prometheus.Counter
	luaFullPathCounters      []prometheus.Counter
	luaNoScriptCounters      []prometheus.Counter
	redisObservability       redisShardObservability
	regionCode               uint8
	evalPinWorkers           int
	evalPins                 *filterEvalPin
	breakers                 []*database.RedisBreaker // M14-04 shard-0 outage reroute
	filterSlowNs             int64                    // M14-17 FILTER_SLOW_MS as nanoseconds; 0 disables
}

// SetFilterSlowMs configures EVALSHA slow-script log threshold (M14-17). Default 5 ms.
func (f *UnifiedFilter) SetFilterSlowMs(ms int) {
	if ms <= 0 {
		f.filterSlowNs = 0
		return
	}
	f.filterSlowNs = int64(ms) * int64(time.Millisecond)
}

// SetPGFallbackAllowed toggles Postgres budget reload on Redis cache miss (disabled in production).
func (f *UnifiedFilter) SetPGFallbackAllowed(allowed bool) {
	f.pgFallbackAllowed = allowed
}

// SetTTCMin configures click fraud time-to-click thresholds for the Lua script.
func (f *UnifiedFilter) SetTTCMin(d time.Duration) {
	f.ttcMinMsAny = d.Milliseconds()
	f.impTsTTLAny = int((10 * time.Minute).Seconds())
}

// SetTTCFailClosed toggles strict TTC enforcement when impression timestamps are missing.
func (f *UnifiedFilter) SetTTCFailClosed(v bool) {
	if v {
		f.ttcFailClosedAny = oneAny
	} else {
		f.ttcFailClosedAny = zeroAny
	}
}

// SetSkipBudgetDebit skips Lua campaign/customer/daily debits when rtb owns authoritative spend.
func (f *UnifiedFilter) SetSkipBudgetDebit(skip bool) {
	if skip {
		f.skipBudgetDebitAny = oneAny
	} else {
		f.skipBudgetDebitAny = zeroAny
	}
}

// SetGeoProvider attaches GeoIP lookup for bid floor enforcement before Lua.
func (f *UnifiedFilter) SetGeoProvider(geo GeoProvider) {
	f.geo = geo
}

// SetGeoBidFloor registers a country-specific minimum bid for pre-Lua validation.
func (f *UnifiedFilter) SetGeoBidFloor(country string, floor int64) {
	f.geoFloors.Store(country, floor)
}

// parseBidMicro reads bid_micro from JSON payloads without full unmarshaling on the track path.
func parseBidMicro(payload []byte) int64 {
	n := len(payload)
	if n < 11 {
		return 0
	}
	_ = payload[n-1]

	for i := 0; i <= n-11; i++ {
		if payload[i] != '"' || loadU64(payload[i:]) != 0x63696d5f64696222 ||
			payload[i+8] != 'r' || payload[i+9] != 'o' {
			continue
		}
		idx := i + 10
		if idx >= n || payload[idx] != '"' {
			continue
		}
		idx++
		for idx < n && (payload[idx] == ' ' || payload[idx] == '\t' || payload[idx] == ':') {
			if payload[idx] == ':' {
				idx++
				break
			}
			idx++
		}

		for idx < n && (payload[idx] == ' ' || payload[idx] == '\t') {
			idx++
		}

		var val int64
		hasDigit := false
		for idx < n && payload[idx] >= '0' && payload[idx] <= '9' {
			val = val*10 + int64(payload[idx]-'0')
			idx++
			hasDigit = true
		}
		if hasDigit {
			return val
		}
		return 0
	}
	return 0
}

// NewUnifiedFilter wires sharded Redis clients, registry, and budget reload paths.
func NewUnifiedFilter(
	rdbs []redis.UniversalClient,
	sharder Sharder,
	registry campaignmodel.CampaignRegistry,
	repo campaignmodel.CampaignRepository,
	rateLimit int,
	rateLimitWindow time.Duration,
	dupTTL time.Duration,
	idempotencyTTL time.Duration,
	clickAmount int64,
	impressionAmount int64,
	streamName string,
	maxStreamLen int,
) *UnifiedFilter {
	script := redis.NewScript(unifiedFilterLua)
	fastScript := redis.NewScript(budgetFastLua)
	return &UnifiedFilter{
		rdbs:                         rdbs,
		sharder:                      sharder,
		script:                       script,
		scriptHash:                   script.Hash(),
		scriptHashAny:                script.Hash(),
		fastScript:                   fastScript,
		fastScriptHashAny:            fastScript.Hash(),
		registry:                     registry,
		repo:                         repo,
		rateLimit:                    rateLimit,
		rateLimitWindow:              rateLimitWindow,
		dupTTL:                       dupTTL,
		idempotencyTTL:               idempotencyTTL,
		clickAmountMicro:             clickAmount,
		impressionAmountMicro:        impressionAmount,
		streamName:                   streamName,
		streamKeyVal:                 StringVal{s: streamName},
		maxStreamLen:                 maxStreamLen,
		rateLimitWindowAny:           int(rateLimitWindow.Seconds()),
		rateLimitAny:                 rateLimit,
		dupTTLAny:                    int(dupTTL.Seconds()),
		idempotencyTTLAny:            int(idempotencyTTL.Seconds()),
		maxStreamLenAny:              maxStreamLen,
		clickAmountMicroAny:          clickAmount,
		impressionAmountMicroAny:     impressionAmount,
		clickAmountMicroHalfAny:      clickAmount / 2,
		impressionAmountMicroHalfAny: impressionAmount / 2,
		ttcFailClosedAny:             zeroAny,
		skipBudgetDebitAny:           zeroAny,
		quotaEnabledAny:              zeroAny,
		quotaChunkSizeAny:            zeroAny,
		quotaRefillThresholdPctAny:   20,
		quotaMode:                    "off",
		localQuotaCache:              NewLocalQuotaCache(),
		luaDurationObservers:         newRedisLuaObservers(len(rdbs)),
		luaFastDurationObservers:     newRedisLuaTierObservers(len(rdbs)),
		luaFastPathCounters:          newRedisLuaPathCounters(len(rdbs), true),
		luaFullPathCounters:          newRedisLuaPathCounters(len(rdbs), false),
		luaNoScriptCounters:          newRedisLuaNoScriptCounters(len(rdbs)),
		redisObservability:           newRedisShardObservability(len(rdbs), luaMetricsSampleMask),
		dbLookupTimeout:              2 * time.Second,
		pgFallbackAllowed:            true,
	}
}

// SetMetricsSampleMask configures downsampling for per-campaign Redis observability counters.
func (f *UnifiedFilter) SetMetricsSampleMask(mask int) {
	f.redisObservability.sampleMask = histogramSampleMaskFromConfig(mask)
}

// SetRegionCode scopes consolidated ingress counters to a regional cell (M9-02).
func (f *UnifiedFilter) SetRegionCode(code uint8) {
	f.regionCode = code
}

// SetLuaFastPathEnabled toggles Tier B budget-fast.lua routing for eligible events.
func (f *UnifiedFilter) SetLuaFastPathEnabled(v bool) {
	f.fastPathEnabled.Store(v)
}

// SetQuotaConfig enables distributed quota keys in unified-filter.lua.
// mode off | shadow | live; off keeps legacy budget:campaign-only path.
func (f *UnifiedFilter) SetQuotaConfig(mode string, chunkSize int64, thresholdPct int) {
	f.quotaMode = mode
	switch mode {
	case "shadow", "live":
		f.quotaEnabledAny = oneAny
	default:
		f.quotaEnabledAny = zeroAny
	}
	f.quotaChunkSizeAny = chunkSize
	if thresholdPct <= 0 {
		thresholdPct = 20
	}
	f.quotaRefillThresholdPctAny = thresholdPct
}

// SetSLATargets configures automatic spend throttling when DB latency exceeds SLA.
func (f *UnifiedFilter) SetSLATargets(p95, recovery float64, stable time.Duration, alpha float64) {
	f.p95ThresholdMs = p95
	f.recoveryEmaMs = recovery
	f.recoveryStableDuration = stable
	f.emaAlpha = alpha
}

// ResizeTrackers reallocates the SLA latency sample ring used by the sentinel.
func (f *UnifiedFilter) ResizeTrackers(size int) {
	f.latencyMu.Lock()
	defer f.latencyMu.Unlock()
	f.latencySamples = make([]float64, size)
	f.latencyIdx = 0
}

// SetDBHealthChecker attaches the Postgres ping target for SLA sentinel monitoring.
func (f *UnifiedFilter) SetDBHealthChecker(checker DBHealthChecker) {
	f.dbHealth = checker
}

// StartSLASentinel runs a background loop that toggles Redis SLA penalty flags.
func (f *UnifiedFilter) StartSLASentinel(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if f.dbHealth == nil {
					continue
				}

				start := time.Now()
				pingCtx, pingCancel := context.WithTimeout(ctx, interval)
				err := f.dbHealth.Ping(pingCtx)
				pingCancel()
				latency := float64(time.Since(start).Milliseconds())
				if err != nil {

					latency = f.p95ThresholdMs + 1000
				}

				f.latencyMu.Lock()
				if len(f.latencySamples) > 0 {
					f.latencySamples[f.latencyIdx%len(f.latencySamples)] = latency
					f.latencyIdx++
				}

				if f.currentEma == 0 {
					f.currentEma = latency
				} else {
					f.currentEma = f.emaAlpha*latency + (1-f.emaAlpha)*f.currentEma
				}

				var p95 float64
				if len(f.latencySamples) > 0 {
					samples := make([]float64, len(f.latencySamples))
					copy(samples, f.latencySamples)
					sort.Float64s(samples)
					idx := int(float64(len(samples)) * 0.95)
					if idx >= len(samples) {
						idx = len(samples) - 1
					}
					p95 = samples[idx]
				}

				isActive := f.slaPenaltyActive.Load()

				if !isActive && p95 > f.p95ThresholdMs {

					for _, rdb := range f.rdbs {
						_ = rdb.Set(ctx, "sla:penalty:active", true, 0).Err()
					}
					f.slaPenaltyActive.Store(true)
				} else if isActive {

					if f.currentEma < f.recoveryEmaMs {
						if f.recoveryStartTime.IsZero() {
							f.recoveryStartTime = time.Now()
						} else if time.Since(f.recoveryStartTime) >= f.recoveryStableDuration {
							for _, rdb := range f.rdbs {
								_ = rdb.Del(ctx, "sla:penalty:active").Err()
							}
							f.slaPenaltyActive.Store(false)
							f.recoveryStartTime = time.Time{}
						}
					} else {
						f.recoveryStartTime = time.Time{}
					}
				}
				f.latencyMu.Unlock()
			}
		}
	}()
}

// checkGeoBidFloor rejects bids below configured country floors before Lua spend.
func (f *UnifiedFilter) checkGeoBidFloor(evt *campaignmodel.Event) error {
	country := evt.GeoCountry
	if country == "" {
		if evt.IngestGeoResolved {
			return nil
		}
		var err error
		country, err = f.geo.GetCountry(evt.IP)
		if err != nil || country == "" {
			return nil
		}
	}
	floorVal, ok := f.geoFloors.Load(country)
	if !ok {
		return nil
	}
	floor := floorVal.(int64)
	if floor <= 0 {
		return nil
	}
	if parseBidMicro(evt.Payload) < floor {
		return ErrBidFloorNotMet
	}
	return nil
}

// Check runs unified Lua; on budget cache miss reloads from registry before Postgres.
func (f *UnifiedFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	nowNano := monotonicNano()
	if f.quotaMode == "live" && f.localQuotaCache.IsBlocked(evt.CampaignID, nowNano) {
		metrics.TrackerLocalQuotaBlockTotal.Inc()
		return ErrBudgetExhausted
	}

	campInfo, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		if reg, ok := f.registry.(*Registry); ok && reg.IsStaleMode() {
			return ErrRegistryStale
		}
		return ErrCampaignNotFound
	}

	if evt.ClickID == "" {
		id := NewFastUUID()
		appendUUID(evt.ClickIDBuf[:0], id)
		evt.ClickID = unsafeString(evt.ClickIDBuf[:])
	}

	if f.geo != nil {
		if err := f.checkGeoBidFloor(evt); err != nil {
			return err
		}
	}

	amount := f.clickAmountMicroAny
	if evt.Type == "impression" {
		amount = f.impressionAmountMicroAny
	}

	if f.slaPenaltyActive.Load() {
		if evt.Type == "impression" {
			amount = f.impressionAmountMicroHalfAny
		} else {
			amount = f.clickAmountMicroHalfAny
		}
	}

	amountMicro := f.impressionAmountMicro
	if evt.Type != "impression" {
		amountMicro = f.clickAmountMicro
	}
	if f.slaPenaltyActive.Load() {
		amountMicro /= 2
	}

	if handled, err := f.checkLocalQuanta(ctx, evt, campInfo, amountMicro); handled {
		return err
	}

	shard, err := f.resolveDebitShard(evt.CampaignID, evt.UserID, campInfo)
	if err != nil {
		return err
	}
	rdb := f.rdbs[shard%len(f.rdbs)]

	if f.fastPathEnabled.Load() && !f.needsFullLuaPath(evt, campInfo) {
		fastScratch := budgetFastScratchPool.Get().(*budgetFastScratch)
		err := f.runBudgetFastLua(ctx, evt, campInfo, amount, rdb, shard, fastScratch)
		budgetFastScratchPool.Put(fastScratch)
		return err
	}

	scratch := unifiedScratchPool.Get().(*unifiedCheckScratch)
	scratch.acquire()
	err = f.runUnifiedLua(ctx, evt, campInfo, amount, rdb, shard, scratch)
	scratch.release()
	unifiedScratchPool.Put(scratch)
	return err
}

func (f *UnifiedFilter) runUnifiedLua(
	ctx context.Context,
	evt *campaignmodel.Event,
	campInfo *campaignmodel.Campaign,
	amount any,
	rdb redis.UniversalClient,
	shard int,
	scratch *unifiedCheckScratch,
) error {
	wDup := &scratch.wDup
	wIdem := &scratch.wIdem
	wDate := &scratch.wDate
	wDS := &scratch.wDS
	wFcap := &scratch.wFcap
	wImpTS := &scratch.wImpTS
	wQuota := &scratch.wQuota
	wRefillLock := &scratch.wRefillLock
	args := scratch.args
	wrappers := &scratch.wrappers
	precheck := &scratch.precheck

	wDup.buf = wDup.buf[:0]
	wDup.buf = appendCampaignHashTag(wDup.buf, evt.CampaignID)
	wDup.buf = append(wDup.buf, "dup:"...)
	wDup.buf = append(wDup.buf, evt.Type...)
	wDup.buf = append(wDup.buf, ':')
	wDup.buf = append(wDup.buf, evt.ClickID...)
	dupKey := unsafeString(wDup.buf)

	budgetSourceKey := campInfo.BudgetCampaignKey

	wIdem.buf = wIdem.buf[:0]
	wIdem.buf = appendCampaignHashTag(wIdem.buf, evt.CampaignID)
	wIdem.buf = append(wIdem.buf, "idempotency:click:"...)
	wIdem.buf = append(wIdem.buf, evt.ClickID...)
	idempotencyKey := unsafeString(wIdem.buf)

	campaignSyncKey := campInfo.CampaignSyncKey
	customerSyncKey := campInfo.CustomerSyncKey

	var now time.Time
	if campInfo.Location == nil || campInfo.Location == time.UTC {
		now = CachedTimeUTC()
	} else {
		now = CachedTimeIn(campInfo.Location)
	}

	wDate.buf = wDate.buf[:0]
	wDate.buf = appendDate(wDate.buf, now)
	currentDate := unsafeString(wDate.buf)

	wDS.buf = wDS.buf[:0]
	wDS.buf = append(wDS.buf, campInfo.DailySpendKeyPrefix...)
	wDS.buf = append(wDS.buf, currentDate...)
	dailySpendKey := unsafeString(wDS.buf)

	if evt.UserID != "" {
		wFcap.buf = wFcap.buf[:0]
		wFcap.buf = append(wFcap.buf, campInfo.FcapKeyPrefix...)
		wFcap.buf = append(wFcap.buf, evt.UserID...)
	}

	wImpTS.buf = wImpTS.buf[:0]
	wImpTS.buf = appendCampaignHashTag(wImpTS.buf, evt.CampaignID)
	wImpTS.buf = append(wImpTS.buf, "imp_ts:"...)
	wImpTS.buf = append(wImpTS.buf, evt.UserID...)
	wImpTS.buf = append(wImpTS.buf, ':')
	wImpTS.buf = appendUUID(wImpTS.buf, evt.CampaignID)
	impTSKey := unsafeString(wImpTS.buf)

	wQuota.buf = wQuota.buf[:0]
	wQuota.buf = appendCampaignHashTag(wQuota.buf, evt.CampaignID)
	wQuota.buf = append(wQuota.buf, "budget:quota:"...)
	wQuota.buf = appendUUID(wQuota.buf, evt.CampaignID)
	quotaKey := unsafeString(wQuota.buf)

	wRefillLock.buf = wRefillLock.buf[:0]
	wRefillLock.buf = appendCampaignHashTag(wRefillLock.buf, evt.CampaignID)
	wRefillLock.buf = append(wRefillLock.buf, "budget:refill_lock:"...)
	wRefillLock.buf = appendUUID(wRefillLock.buf, evt.CampaignID)
	refillLockKey := unsafeString(wRefillLock.buf)

	wFence := &scratch.wFence
	wFence.buf = wFence.buf[:0]
	wFence.buf = append(wFence.buf, MigrationFenceKeyPrefix...)
	wFence.buf = appendUUID(wFence.buf, evt.CampaignID)
	migrationFenceKey := unsafeString(wFence.buf)

	wFrozen := &scratch.wFrozen
	wFrozen.buf = wFrozen.buf[:0]
	wFrozen.buf = append(wFrozen.buf, BudgetFrozenKeyPrefix...)
	wFrozen.buf = appendUUID(wFrozen.buf, evt.CampaignID)
	budgetFrozenKey := unsafeString(wFrozen.buf)

	kv := scratch.keyVals[:]
	kv[0].s = fraudBlacklistKey
	kv[1].s = dupKey
	kv[2].s = budgetSourceKey
	kv[3].s = idempotencyKey
	kv[4].s = campaignSyncKey
	kv[5].s = customerSyncKey
	kv[9].s = dailySpendKey
	kv[11].s = impTSKey
	kv[12].s = quotaKey
	kv[13].s = refillLockKey
	kv[14].s = migrationFenceKey
	kv[15].s = budgetFrozenKey

	keyArgs := scratch.keyArgs
	keyArgs[0] = &kv[0]
	keyArgs[1] = &kv[1]
	keyArgs[2] = &kv[2]
	keyArgs[3] = &kv[3]
	keyArgs[4] = &kv[4]
	keyArgs[5] = &kv[5]
	keyArgs[6] = &dirtyCampaignsKeyVal
	keyArgs[7] = &dirtyCustomersKeyVal
	keyArgs[8] = &f.streamKeyVal
	keyArgs[9] = &kv[9]
	keyArgs[11] = &kv[11]
	keyArgs[12] = &kv[12]
	keyArgs[13] = &kv[13]
	keyArgs[14] = &refillNeededKeyVal
	keyArgs[15] = &kv[14]
	keyArgs[16] = &kv[15]
	maxRPDAny := f.fillLuaPrecheckKeys(evt, campInfo, now, precheck, kv[:], keyArgs[:], 17, 18)
	if evt.UserID != "" {
		kv[10].s = unsafeString(wFcap.buf)
		keyArgs[10] = &kv[10]
	} else {
		keyArgs[10] = &fcapIgnoredKeyVal
	}

	isEven := zeroAny
	if campInfo.PacingMode == campaignmodel.PacingModeEven {
		isEven = oneAny
	}

	hr := now.Hour() + 1
	if hr < 1 {
		hr = 1
	} else if hr > 24 {
		hr = 24
	}
	currentHour := hourAnyCache[hr]

	wrappers.clickID.s = evt.ClickID
	wrappers.evtType.s = evt.Type
	wrappers.payload.s = unsafeString(evt.Payload)
	wrappers.ip.s = evt.IP
	wrappers.ua.s = evt.UA
	wrappers.userID.s = evt.UserID
	wrappers.placementID.s = evt.PlacementID

	args[0] = zeroAny // M9-03: IP rate limit enforced at edge only
	args[1] = zeroAny
	args[2] = f.dupTTLAny
	args[3] = amount
	args[4] = f.idempotencyTTLAny
	args[5] = campInfo.IDStrAny
	args[6] = campInfo.CustomerIDStrAny
	args[7] = f.maxStreamLenAny
	args[8] = &wrappers.clickID
	args[9] = &wrappers.evtType
	args[10] = &wrappers.payload
	args[11] = &wrappers.ip
	args[12] = &wrappers.ua
	args[13] = isEven
	args[14] = campInfo.DailyBudgetMicroAny
	args[15] = currentHour
	args[16] = &wrappers.userID
	args[17] = campInfo.FreqLimitAny
	args[18] = campInfo.FreqWindowAny
	args[19] = f.ttcMinMsAny
	args[20] = cachedUnixMilliAny.Load()
	args[21] = f.impTsTTLAny
	args[22] = f.ttcFailClosedAny
	args[23] = f.skipBudgetDebitAny
	args[24] = f.quotaEnabledAny
	args[25] = f.quotaChunkSizeAny
	args[26] = f.quotaRefillThresholdPctAny
	args[27] = campInfo.LuaRoutingEpoch()
	if evt == nil || evt.FilterDeadlineMono <= 0 {
		args[28] = zeroAny
		args[29] = zeroAny
	} else {
		wD := &scratch.wDeadlineMono
		wD.buf = strconv.AppendInt(wD.buf[:0], evt.FilterDeadlineMono, 10)
		scratch.deadlineMonoStr.s = unsafeString(wD.buf)
		args[28] = &scratch.deadlineMonoStr
		wN := &scratch.wNowMono
		wN.buf = strconv.AppendInt(wN.buf[:0], monotonicNano(), 10)
		scratch.nowMonoStr.s = unsafeString(wN.buf)
		args[29] = &scratch.nowMonoStr
	}
	args[30] = luaDegradeThresholdAny
	args[31] = &wrappers.placementID
	args[32] = maxRPDAny
	args[33] = luaPrecheckIngressTTLAny

	for i := 0; i < 2; i++ {
		seq := f.luaMetricsSeq.Add(1)
		sampleLua := shouldSampleHistogram(seq, f.redisObservability.sampleMask)
		var luaStart int64
		if sampleLua || f.filterSlowNs > 0 {
			luaStart = monotonicNano()
		}
		f.redisObservability.recordLuaOp(shard, evt.CampaignID, sampleLua)
		incRedisLuaTier(f.luaFullPathCounters, shard)
		res, err := f.evalScript(ctx, rdb, shard, evt, keyArgs, args[:34])

		f.noteLuaEvalDuration(shard, evt.CampaignID, "full", luaStart, sampleLua, false)

		if err != nil {
			return err
		}

		if res == -1 {
			retry, recErr := f.recoverBudgetAfterMiss(ctx, evt, rdb, budgetSourceKey, i)
			if recErr != nil {
				return recErr
			}
			if retry {
				continue
			}
			return ErrBudgetExhausted
		}

		if handled, handleErr := f.handleLuaResult(ctx, evt, campInfo, amount, rdb, budgetSourceKey, shard, res, sampleLua); handled {
			return handleErr
		}
	}

	return nil
}

// recordAcceptedSpendIfDebited emits sampled spend only when Lua debited budget.
func (f *UnifiedFilter) recordAcceptedSpendIfDebited(shard int, campaignID uuid.UUID, amount any, sample bool) {
	if f.skipBudgetDebitAny == oneAny {
		return
	}
	f.redisObservability.recordAcceptedSpend(shard, campaignID, spendMicroFromAny(amount), sample)
}

// spendMicroFromAny extracts the Lua debit amount from pre-boxed int64 args.
func spendMicroFromAny(amount any) int64 {
	v, ok := amount.(int64)
	if !ok {
		return 0
	}
	return v
}
