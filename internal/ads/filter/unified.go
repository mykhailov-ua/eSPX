package filter

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"espx/internal/ads/catalog"
	"espx/internal/ads/clock"
	"espx/internal/ads/sharding"
	"espx/internal/domain"
	"espx/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	redis "github.com/redis/go-redis/v9"
)

// unifiedFilterLua holds the Redis script that enforces budget, pacing, dedup, and stream enqueue in one round trip.
//
//go:embed unified.lua
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
	clickID StringVal
	evtType StringVal
	payload StringVal
	ip      StringVal
	ua      StringVal
	userID  StringVal
}

var (
	dirtyCampaignsKeyVal = StringVal{s: "budget:dirty_campaigns"}
	dirtyCustomersKeyVal = StringVal{s: "budget:dirty_customers"}
	refillNeededKeyVal   = StringVal{s: "budget:refill_needed"}
	fcapIgnoredKeyVal    = StringVal{s: "fcap:ignored"}
)

// unifiedCheckScratch holds pooled buffers for one UnifiedFilter.Check without defer.
type unifiedCheckScratch struct {
	wRL, wDup, wIdem, wDate, wDS, wFcap, wImpTS, wQuota, wRefillLock bufWrapper
	args                                                             []any
	wrappers                                                         UnifiedStringWrappers
	keyVals                                                          [unifiedFilterKeyCount]StringVal
	keyArgs                                                          [unifiedFilterKeyCount]any
}

var unifiedScratchPool = sync.Pool{
	New: func() any {
		s := &unifiedCheckScratch{
			args: make([]any, 28),
		}
		s.wRL.Buf = make([]byte, 0, 128)
		s.wDup.Buf = make([]byte, 0, 128)
		s.wIdem.Buf = make([]byte, 0, 128)
		s.wDate.Buf = make([]byte, 0, 128)
		s.wDS.Buf = make([]byte, 0, 128)
		s.wFcap.Buf = make([]byte, 0, 128)
		s.wImpTS.Buf = make([]byte, 0, 128)
		s.wQuota.Buf = make([]byte, 0, 128)
		s.wRefillLock.Buf = make([]byte, 0, 128)
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
	for i := 0; i <= 24; i++ {
		hourAnyCache[i] = i
	}
}

// DBHealthChecker supports SLA sentinel latency probes against Postgres.
type DBHealthChecker interface {
	Ping(ctx context.Context) error
}

// UnifiedFilter runs budget, pacing, dedup, and stream enqueue in one Redis Lua round trip.
type UnifiedFilter struct {
	rdbs                     []redis.UniversalClient
	sharder                  sharding.Sharder
	script                   *redis.Script
	scriptHash               string
	scriptHashAny            any
	registry                 domain.CampaignRegistry
	repo                     domain.CampaignRepository
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
	localQuotaCache              *LocalQuotaCache
	dbLookupTimeout              time.Duration
	luaMetricsSeq                atomic.Uint64

	luaDurationObservers []prometheus.Observer
	luaNoScriptCounters  []prometheus.Counter
	redisObservability   redisShardObservability
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

// parseBidMicroKey is the JSON field prefix scanned without full unmarshaling.
var parseBidMicroKey = []byte(`"bid_micro"`)

// ParseBidMicro reads bid_micro from JSON payloads without full unmarshaling on the track path.
func ParseBidMicro(payload []byte) int64 {
	return parseBidMicro(payload)
}

// parseBidMicro reads bid_micro from JSON payloads without full unmarshaling on the track path.
func parseBidMicro(payload []byte) int64 {
	n := len(payload)
	kLen := len(parseBidMicroKey)
	if n < kLen {
		return 0
	}

	for i := 0; i <= n-kLen; i++ {
		if payload[i] == '"' && bytes.Equal(payload[i:i+kLen], parseBidMicroKey) {
			idx := i + kLen
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
	}
	return 0
}

// NewUnifiedFilter constructs the primary tracker filter with sharded Redis clients.
func NewUnifiedFilter(
	rdbs []redis.UniversalClient,
	sharder sharding.Sharder,
	registry domain.CampaignRegistry,
	repo domain.CampaignRepository,
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
	return &UnifiedFilter{
		rdbs:                         rdbs,
		sharder:                      sharder,
		script:                       script,
		scriptHash:                   script.Hash(),
		scriptHashAny:                script.Hash(),
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
		luaNoScriptCounters:          newRedisLuaNoScriptCounters(len(rdbs)),
		redisObservability:           newRedisShardObservability(len(rdbs), luaMetricsSampleMask),
		dbLookupTimeout:              2 * time.Second,
	}
}

// SetMetricsSampleMask configures downsampling for per-campaign Redis observability counters.
func (f *UnifiedFilter) SetMetricsSampleMask(mask int) {
	f.redisObservability.sampleMask = histogramSampleMaskFromConfig(mask)
}

// SetQuotaConfig enables distributed quota keys in unified-filter.lua (Phase 1.3).
// mode: off | shadow | live — off keeps legacy budget:campaign-only path.
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

// SetDBLookupTimeoutForTest overrides Postgres budget-miss lookup timeout in tests.
func (f *UnifiedFilter) SetDBLookupTimeoutForTest(d time.Duration) {
	f.dbLookupTimeout = d
}

// SLAPenaltyActiveForTest reports in-memory SLA penalty flag for cross-package tests.
func (f *UnifiedFilter) SLAPenaltyActiveForTest() bool {
	return f.slaPenaltyActive.Load()
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

// getRDB selects the Redis shard for a campaign so Lua keys stay colocated with budget state.
func (f *UnifiedFilter) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(f.rdbs) <= 1 {
		return f.rdbs[0]
	}
	idx := f.sharder.GetShard(campaignID)
	return f.rdbs[idx%len(f.rdbs)]
}

// checkGeoBidFloor rejects bids below configured country floors before Lua spend.
func (f *UnifiedFilter) checkGeoBidFloor(evt *domain.Event) error {
	country, err := f.geo.GetCountry(evt.IP)
	if err != nil || country == "" {
		return nil
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

// Check runs the unified Lua filter, reloading budget from registry or Postgres on cache miss.
func (f *UnifiedFilter) Check(ctx context.Context, evt *domain.Event) error {
	nowNano := time.Now().UnixNano()
	if f.quotaMode == "live" && f.localQuotaCache.IsBlocked(evt.CampaignID, nowNano) {
		metrics.TrackerLocalQuotaBlockTotal.WithLabelValues(evt.CampaignID.String()).Inc()
		return ErrBudgetExhausted
	}

	campInfo, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}

	if evt.ClickID == "" {
		id, err := clock.NewFastUUID()
		if err != nil {
			return fmt.Errorf("failed to generate click id: %w", err)
		}
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

	rdb := f.getRDB(evt.CampaignID)

	scratch := unifiedScratchPool.Get().(*unifiedCheckScratch)
	scratch.acquire()
	err := f.runUnifiedLua(ctx, evt, campInfo, amount, rdb, scratch)
	scratch.release()
	unifiedScratchPool.Put(scratch)
	return err
}

func (f *UnifiedFilter) runUnifiedLua(
	ctx context.Context,
	evt *domain.Event,
	campInfo *domain.Campaign,
	amount any,
	rdb redis.UniversalClient,
	scratch *unifiedCheckScratch,
) error {
	wRL := &scratch.wRL
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

	wRL.Buf = wRL.Buf[:0]
	wRL.Buf = append(wRL.Buf, "rl:ip:"...)
	wRL.Buf = append(wRL.Buf, evt.IP...)
	rlKey := unsafeString(wRL.Buf)

	wDup.Buf = wDup.Buf[:0]
	wDup.Buf = append(wDup.Buf, "dup:"...)
	wDup.Buf = append(wDup.Buf, evt.Type...)
	wDup.Buf = append(wDup.Buf, ':')
	wDup.Buf = append(wDup.Buf, evt.ClickID...)
	dupKey := unsafeString(wDup.Buf)

	budgetSourceKey := campInfo.BudgetCampaignKey

	wIdem.Buf = wIdem.Buf[:0]
	wIdem.Buf = append(wIdem.Buf, "idempotency:click:"...)
	wIdem.Buf = append(wIdem.Buf, evt.ClickID...)
	idempotencyKey := unsafeString(wIdem.Buf)

	campaignSyncKey := campInfo.CampaignSyncKey
	customerSyncKey := campInfo.CustomerSyncKey

	var now time.Time
	if campInfo.Location == nil || campInfo.Location == time.UTC {
		now = clock.CachedTimeUTC()
	} else {
		now = clock.CachedTimeIn(campInfo.Location)
	}

	wDate.Buf = wDate.Buf[:0]
	wDate.Buf = appendDate(wDate.Buf, now)
	currentDate := unsafeString(wDate.Buf)

	wDS.Buf = wDS.Buf[:0]
	wDS.Buf = append(wDS.Buf, campInfo.DailySpendKeyPrefix...)
	wDS.Buf = append(wDS.Buf, currentDate...)
	dailySpendKey := unsafeString(wDS.Buf)

	if evt.UserID != "" {
		wFcap.Buf = wFcap.Buf[:0]
		wFcap.Buf = append(wFcap.Buf, campInfo.FcapKeyPrefix...)
		wFcap.Buf = append(wFcap.Buf, evt.UserID...)
	}

	wImpTS.Buf = wImpTS.Buf[:0]
	wImpTS.Buf = append(wImpTS.Buf, "imp_ts:"...)
	wImpTS.Buf = append(wImpTS.Buf, evt.UserID...)
	wImpTS.Buf = append(wImpTS.Buf, ':')
	wImpTS.Buf = appendUUID(wImpTS.Buf, evt.CampaignID)
	impTSKey := unsafeString(wImpTS.Buf)

	wQuota.Buf = wQuota.Buf[:0]
	wQuota.Buf = append(wQuota.Buf, "budget:quota:"...)
	wQuota.Buf = appendUUID(wQuota.Buf, evt.CampaignID)
	quotaKey := unsafeString(wQuota.Buf)

	wRefillLock.Buf = wRefillLock.Buf[:0]
	wRefillLock.Buf = append(wRefillLock.Buf, "budget:refill_lock:"...)
	wRefillLock.Buf = appendUUID(wRefillLock.Buf, evt.CampaignID)
	refillLockKey := unsafeString(wRefillLock.Buf)

	kv := scratch.keyVals[:]
	kv[0].s = rlKey
	kv[1].s = dupKey
	kv[2].s = budgetSourceKey
	kv[3].s = idempotencyKey
	kv[4].s = campaignSyncKey
	kv[5].s = customerSyncKey
	kv[9].s = dailySpendKey
	kv[11].s = impTSKey
	kv[12].s = quotaKey
	kv[13].s = refillLockKey

	keyArgs := scratch.keyArgs
	keyArgs[6] = &dirtyCampaignsKeyVal
	keyArgs[7] = &dirtyCustomersKeyVal
	keyArgs[8] = &f.streamKeyVal
	keyArgs[14] = &refillNeededKeyVal
	if evt.UserID != "" {
		kv[10].s = unsafeString(wFcap.Buf)
		keyArgs[10] = &kv[10]
	} else {
		keyArgs[10] = &fcapIgnoredKeyVal
	}

	isEven := zeroAny
	if campInfo.PacingMode == domain.PacingModeEven {
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

	args[0] = f.rateLimitWindowAny
	args[1] = f.rateLimitAny
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
	args[20] = clock.CachedUnixMilliAny.Load()
	args[21] = f.impTsTTLAny
	args[22] = f.ttcFailClosedAny
	args[23] = f.skipBudgetDebitAny
	args[24] = f.quotaEnabledAny
	args[25] = f.quotaChunkSizeAny
	args[26] = f.quotaRefillThresholdPctAny

	shard := f.sharder.GetShard(evt.CampaignID)
	for i := 0; i < 2; i++ {
		seq := f.luaMetricsSeq.Add(1)
		sampleLua := shouldSampleHistogram(seq, f.redisObservability.sampleMask)
		var luaStart int64
		if sampleLua {
			luaStart = monotonicNano()
		}
		f.redisObservability.recordLuaOp(shard, evt.CampaignID, sampleLua)
		res, err := f.evalScript(ctx, rdb, shard, keyArgs, args)

		if sampleLua {
			observeRedisLua(f.luaDurationObservers, shard, MonoElapsedSeconds(luaStart))
		}

		if err != nil {
			return err
		}

		if res == -1 {
			metrics.BudgetCacheMissTotal.Inc()
			if filterDeadlineExceededEvt(evt, ctx) {
				return context.DeadlineExceeded
			}
			if i > 0 {
				return fmt.Errorf("budget cache miss on retry: %w", ErrBudgetExhausted)
			}

			recovered, recErr := catalog.TryRecoverBudgetFromRegistry(ctx, rdb, f.registry, evt.CampaignID, budgetSourceKey)
			if recErr != nil {
				return recErr
			}
			if recovered {
				continue
			}

			dbTimeout := f.dbLookupTimeout
			if rem, ok := filterDeadlineRemainingEvt(evt, ctx); ok {
				if rem <= 0 {
					return context.DeadlineExceeded
				}
				if rem < dbTimeout {
					dbTimeout = rem
				}
			}

			metrics.BudgetCacheMissPGTotal.Inc()
			dbCtx, cancel := context.WithTimeout(ctx, dbTimeout)
			camp, err := f.repo.GetByID(dbCtx, evt.CampaignID)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return context.DeadlineExceeded
				}
				return fmt.Errorf("failed to load campaign from db: %w", err)
			}

			remaining := camp.BudgetLimit - camp.CurrentSpend
			if remaining < 0 {
				remaining = 0
			}

			if err := catalog.WarmBudgetKeyNX(ctx, rdb, budgetSourceKey, remaining); err != nil {
				return fmt.Errorf("warm budget key after pg load: %w", err)
			}
			continue
		}

		switch res {
		case 1:
			return ErrRateLimitExceeded
		case 2:
			return ErrDuplicateEvent
		case 3:
			if f.quotaMode == "live" {
				f.localQuotaCache.Block(evt.CampaignID, time.Now().UnixNano())
			}
			return ErrBudgetExhausted
		case 4:
			return ErrPacingExhausted
		case 5:
			return ErrFreqLimitExceeded
		case 6:
			addFraudSignal(evt, FraudReasonLowTTC)
			return nil
		case 7:
			addFraudSignal(evt, FraudReasonMissingImpTS)
			return nil
		case 10:
			metrics.TTCBypassTotal.Inc()
			metrics.EventsProcessed.Inc()
			f.recordAcceptedSpendIfDebited(shard, evt.CampaignID, amount, sampleLua)
			return nil
		default:
			metrics.EventsProcessed.Inc()
			f.recordAcceptedSpendIfDebited(shard, evt.CampaignID, amount, sampleLua)
			return nil
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
