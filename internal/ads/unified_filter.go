package ads

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

	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	redis "github.com/redis/go-redis/v9"
)

//go:embed unified_filter.lua
var unifiedFilterLua string

// keysPool recycles Redis key slices for unified filter Lua calls.
var keysPool = sync.Pool{
	New: func() any {
		s := make([]string, 12)
		return &s
	},
}

// argsPool recycles Redis argument slices for unified filter Lua calls.
var argsPool = sync.Pool{
	New: func() any {
		s := make([]any, 23)
		return &s
	},
}

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

// unifiedWrappersPool recycles string wrapper structs for unified filter checks.
var unifiedWrappersPool = sync.Pool{
	New: func() any {
		return &UnifiedStringWrappers{}
	},
}

// appendDate formats YYYYMMDD into dst without time.Format allocations.
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
	sharder                  Sharder
	script                   *redis.Script
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
	dbLookupTimeout              time.Duration
	luaMetricsSeq                atomic.Uint64

	luaDurationObservers []prometheus.Observer
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

// parseBidMicro extracts bid_micro from JSON payload bytes on the hot path.
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
	sharder Sharder,
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
	return &UnifiedFilter{
		rdbs:                         rdbs,
		sharder:                      sharder,
		script:                       redis.NewScript(unifiedFilterLua),
		registry:                     registry,
		repo:                         repo,
		rateLimit:                    rateLimit,
		rateLimitWindow:              rateLimitWindow,
		dupTTL:                       dupTTL,
		idempotencyTTL:               idempotencyTTL,
		clickAmountMicro:             clickAmount,
		impressionAmountMicro:        impressionAmount,
		streamName:                   streamName,
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
		luaDurationObservers:         newRedisLuaObservers(len(rdbs)),
		dbLookupTimeout:              2 * time.Second,
	}
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

// getRDB returns the Redis client shard for a campaign id.
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
	campInfo, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}

	if evt.ClickID == "" {
		id, err := NewFastUUID()
		if err != nil {
			return fmt.Errorf("failed to generate click id: %w", err)
		}
		evt.ClickID = id.String()
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

	wRL := bufPool.Get().(*bufWrapper)
	wDup := bufPool.Get().(*bufWrapper)
	wIdem := bufPool.Get().(*bufWrapper)
	wDate := bufPool.Get().(*bufWrapper)
	wDS := bufPool.Get().(*bufWrapper)
	wFcap := bufPool.Get().(*bufWrapper)
	wImpTS := bufPool.Get().(*bufWrapper)
	keysPtr := keysPool.Get().(*[]string)
	argsPtr := argsPool.Get().(*[]any)
	wrappers := unifiedWrappersPool.Get().(*UnifiedStringWrappers)

	defer func() {
		bufPool.Put(wRL)
		bufPool.Put(wDup)
		bufPool.Put(wIdem)
		bufPool.Put(wDate)
		bufPool.Put(wDS)
		bufPool.Put(wFcap)
		bufPool.Put(wImpTS)
		keysPool.Put(keysPtr)
		argsPool.Put(argsPtr)
		unifiedWrappersPool.Put(wrappers)
	}()

	wRL.buf = wRL.buf[:0]
	wRL.buf = append(wRL.buf, "rl:ip:"...)
	wRL.buf = append(wRL.buf, evt.IP...)
	rlKey := unsafeString(wRL.buf)

	wDup.buf = wDup.buf[:0]
	wDup.buf = append(wDup.buf, "dup:"...)
	wDup.buf = append(wDup.buf, evt.Type...)
	wDup.buf = append(wDup.buf, ':')
	wDup.buf = append(wDup.buf, evt.ClickID...)
	dupKey := unsafeString(wDup.buf)

	budgetSourceKey := campInfo.BudgetCampaignKey

	wIdem.buf = wIdem.buf[:0]
	wIdem.buf = append(wIdem.buf, "idempotency:click:"...)
	wIdem.buf = append(wIdem.buf, evt.ClickID...)
	idempotencyKey := unsafeString(wIdem.buf)

	campaignSyncKey := campInfo.CampaignSyncKey
	customerSyncKey := campInfo.CustomerSyncKey

	dirtyCampaignsKey := "budget:dirty_campaigns"
	dirtyCustomersKey := "budget:dirty_customers"
	streamKey := f.streamName

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

	var fcapKey string
	if evt.UserID != "" {
		wFcap.buf = wFcap.buf[:0]
		wFcap.buf = append(wFcap.buf, campInfo.FcapKeyPrefix...)
		wFcap.buf = append(wFcap.buf, evt.UserID...)
		fcapKey = unsafeString(wFcap.buf)
	} else {
		fcapKey = "fcap:ignored"
	}

	wImpTS.buf = wImpTS.buf[:0]
	wImpTS.buf = append(wImpTS.buf, "imp_ts:"...)
	wImpTS.buf = append(wImpTS.buf, evt.UserID...)
	wImpTS.buf = append(wImpTS.buf, ':')
	wImpTS.buf = appendUUID(wImpTS.buf, evt.CampaignID)
	impTSKey := unsafeString(wImpTS.buf)

	keys := *keysPtr
	keys[0] = rlKey
	keys[1] = dupKey
	keys[2] = budgetSourceKey
	keys[3] = idempotencyKey
	keys[4] = campaignSyncKey
	keys[5] = customerSyncKey
	keys[6] = dirtyCampaignsKey
	keys[7] = dirtyCustomersKey
	keys[8] = streamKey
	keys[9] = dailySpendKey
	keys[10] = fcapKey
	keys[11] = impTSKey

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

	args := *argsPtr
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
	args[20] = cachedUnixMilli.Load()
	args[21] = f.impTsTTLAny
	args[22] = f.ttcFailClosedAny

	shard := f.sharder.GetShard(evt.CampaignID)
	for i := 0; i < 2; i++ {
		seq := f.luaMetricsSeq.Add(1)
		sampleLua := shouldSampleLuaMetrics(seq)
		var luaStart int64
		if sampleLua {
			luaStart = monotonicNano()
		}
		cmd := f.evalScript(ctx, rdb, shard, keys, args)

		if sampleLua {
			observeRedisLua(f.luaDurationObservers, shard, monoElapsedSeconds(luaStart))
		}
		res, err := cmd.Int64()

		if err != nil {
			return err
		}

		if res == -1 {
			metrics.BudgetCacheMissTotal.Inc()
			if filterDeadlineExceeded(ctx) {
				return context.DeadlineExceeded
			}
			if i > 0 {
				return fmt.Errorf("budget cache miss on retry: %w", ErrBudgetExhausted)
			}

			recovered, recErr := tryRecoverBudgetFromRegistry(ctx, rdb, f.registry, evt.CampaignID, budgetSourceKey)
			if recErr != nil {
				return recErr
			}
			if recovered {
				continue
			}

			dbTimeout := f.dbLookupTimeout
			if rem, ok := filterDeadlineRemaining(ctx); ok {
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

			if err := warmBudgetKeyNX(ctx, rdb, budgetSourceKey, remaining); err != nil {
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
			return ErrBudgetExhausted
		case 4:
			return ErrPacingExhausted
		case 5:
			return ErrFreqLimitExceeded
		case 6:
			evt.FraudReason = "low_ttc"
			return ErrFraudDetected
		case 7:
			evt.FraudReason = "missing_imp_ts"
			return ErrFraudDetected
		case 10:
			metrics.TTCBypassTotal.Inc()
			metrics.EventsProcessed.Inc()
			return nil
		default:
			metrics.EventsProcessed.Inc()
			return nil
		}
	}

	return nil
}
