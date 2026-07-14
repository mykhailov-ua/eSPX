package ads

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"espx/internal/domain"
)

// filterDeadlineKey tags context with a monotonic filter deadline independent of wall clock.
type filterDeadlineKey struct{}

const maxFraudSignals = 4

// FraudTier is the campaign-relative outcome band for an accumulated fraud score.
type FraudTier uint8

const (
	FraudTierPass FraudTier = iota
	FraudTierSuspect
	FraudTierIVT
	FraudTierBlock
)

// fraudAccumulator collects weighted fraud signals during one FilterEngine.Check pass.
type fraudAccumulator struct {
	score        uint32
	signals      [maxFraudSignals]FraudReasonID
	count        uint8
	boostApplied bool
}

var fraudAccPool = sync.Pool{
	New: func() any {
		return &fraudAccumulator{}
	},
}

func (a *fraudAccumulator) reset() {
	a.score = 0
	a.count = 0
	a.boostApplied = false
}

func (a *fraudAccumulator) has(id FraudReasonID) bool {
	for i := uint8(0); i < a.count; i++ {
		if a.signals[i] == id {
			return true
		}
	}
	return false
}

func (a *fraudAccumulator) add(id FraudReasonID) {
	if id == FraudReasonNone || id >= fraudReasonCount || a.has(id) {
		return
	}
	weight := FraudSignalWeight(id)
	if weight == 0 {
		return
	}
	if a.count >= maxFraudSignals {
		return
	}
	a.signals[a.count] = id
	a.count++
	sum := a.score + uint32(weight)
	if sum > 100 {
		sum = 100
	}
	a.score = sum
}

func (a *fraudAccumulator) countFlags(want uint8) uint8 {
	if a == nil || want == 0 {
		return 0
	}
	var n uint8
	for i := uint8(0); i < a.count; i++ {
		if FraudSignalFlags(a.signals[i])&want != 0 {
			n++
		}
	}
	return n
}

func (a *fraudAccumulator) hasFlags(want uint8) bool {
	return a.countFlags(want) > 0
}

// shouldShortCircuitFraudBudget reports whether unified Lua can be skipped after L3 or two L1-high signals.
func (a *fraudAccumulator) shouldShortCircuitFraudBudget() bool {
	if a == nil || a.count == 0 {
		return false
	}
	if a.hasFlags(fraudSignalL3) {
		return true
	}
	return a.countFlags(fraudSignalL1High) >= 2
}

// attachFilterDeadline attaches a monotonic deadline shared by all filter checks in one request.
func attachFilterDeadline(ctx context.Context, timeout time.Duration) context.Context {
	if timeout <= 0 {
		return ctx
	}
	deadlineMono := monotonicNano() + timeout.Nanoseconds()
	return context.WithValue(ctx, filterDeadlineKey{}, deadlineMono)
}

// setFilterDeadlineOnEvent stores the filter deadline on evt (zero allocs; production path).
func setFilterDeadlineOnEvent(evt *domain.Event, timeout time.Duration) {
	if evt != nil && timeout > 0 {
		evt.FilterDeadlineMono = monotonicNano() + timeout.Nanoseconds()
	}
}

// attachFraudAccumulator binds a pooled accumulator to evt.Scratch for the current Check pass.
func attachFraudAccumulator(evt *domain.Event) *fraudAccumulator {
	acc := fraudAccPool.Get().(*fraudAccumulator)
	acc.reset()
	if evt != nil {
		evt.Scratch = unsafe.Pointer(acc)
	}
	return acc
}

// releaseFraudAccumulator clears Scratch and returns the accumulator to fraudAccPool.
func releaseFraudAccumulator(evt *domain.Event, acc *fraudAccumulator) {
	if acc == nil {
		return
	}
	acc.reset()
	fraudAccPool.Put(acc)
	if evt != nil {
		evt.Scratch = nil
	}
}

// fraudAccFromEvent loads the accumulator stored in evt.Scratch during FilterEngine.Check.
func fraudAccFromEvent(evt *domain.Event) (*fraudAccumulator, bool) {
	if evt == nil || evt.Scratch == nil {
		return nil, false
	}
	return (*fraudAccumulator)(evt.Scratch), true
}

// addFraudSignal records a weighted fraud signal for the current filter pass.
func addFraudSignal(evt *domain.Event, id FraudReasonID) {
	acc, ok := fraudAccFromEvent(evt)
	if !ok {
		return
	}
	acc.add(id)
}

// MapFraudTier maps a fraud score to a tier. Zero boundaries use domain defaults.
func MapFraudTier(score uint8, pass, suspect, ivt, block uint8) FraudTier {
	if pass == 0 && suspect == 0 && ivt == 0 {
		pass = domain.DefaultFraudThresholdPass
		suspect = domain.DefaultFraudThresholdSuspect
		ivt = domain.DefaultFraudThresholdIVT
	}
	if score <= pass {
		return FraudTierPass
	}
	if score <= suspect {
		return FraudTierSuspect
	}
	if score <= ivt {
		return FraudTierIVT
	}
	_ = block // persisted on campaigns; scores above ivt always map to Block.
	return FraudTierBlock
}

// fraudThresholdsFromCampaign returns tier boundaries from camp or domain defaults when unset.
func fraudThresholdsFromCampaign(camp *domain.Campaign) (pass, suspect, ivt, block uint8) {
	if camp == nil {
		return domain.DefaultFraudThresholdPass, domain.DefaultFraudThresholdSuspect,
			domain.DefaultFraudThresholdIVT, domain.DefaultFraudThresholdBlock
	}
	pass = camp.FraudThresholdPass
	suspect = camp.FraudThresholdSuspect
	ivt = camp.FraudThresholdIVT
	block = camp.FraudThresholdBlock
	if pass == 0 && suspect == 0 && ivt == 0 && block == 0 {
		return domain.DefaultFraudThresholdPass, domain.DefaultFraudThresholdSuspect,
			domain.DefaultFraudThresholdIVT, domain.DefaultFraudThresholdBlock
	}
	return pass, suspect, ivt, block
}

// applyFraudAccumulatorForCampaign writes FraudScore, comma-separated FraudReason, and returns the mapped tier.
func applyFraudAccumulatorForCampaign(evt *domain.Event, acc *fraudAccumulator, camp *domain.Campaign) FraudTier {
	if evt == nil || acc == nil || acc.count == 0 {
		if evt != nil {
			evt.FraudScore = 0
			evt.FraudReason = ""
		}
		return FraudTierPass
	}

	evt.FraudScore = acc.score

	totalLen := 0
	for i := uint8(0); i < acc.count; i++ {
		if i > 0 {
			totalLen++
		}
		totalLen += len(FraudReasonCode(acc.signals[i]))
	}
	if cap(evt.StringBuffer) < totalLen {
		evt.StringBuffer = make([]byte, 0, totalLen+16)
	} else {
		evt.StringBuffer = evt.StringBuffer[:0]
	}
	for i := uint8(0); i < acc.count; i++ {
		if i > 0 {
			evt.StringBuffer = append(evt.StringBuffer, ',')
		}
		evt.StringBuffer = append(evt.StringBuffer, FraudReasonCode(acc.signals[i])...)
	}
	evt.FraudReason = unsafeString(evt.StringBuffer)

	pass, suspect, ivt, block := fraudThresholdsFromCampaign(camp)
	return MapFraudTier(uint8(acc.score), pass, suspect, ivt, block)
}

// filterDeadlineMonoEvt returns the monotonic deadline from evt or ctx.
func filterDeadlineMonoEvt(evt *domain.Event, ctx context.Context) (int64, bool) {
	if evt != nil && evt.FilterDeadlineMono > 0 {
		return evt.FilterDeadlineMono, true
	}
	return filterDeadlineMonoFromContext(ctx)
}

// filterDeadlineExceededEvt reports whether the filter deadline on evt or ctx has elapsed.
func filterDeadlineExceededEvt(evt *domain.Event, ctx context.Context) bool {
	if d, ok := filterDeadlineMonoEvt(evt, ctx); ok {
		return monotonicNano() > d
	}
	return false
}

// filterDeadlineRemainingEvt returns remaining filter budget from evt or ctx.
func filterDeadlineRemainingEvt(evt *domain.Event, ctx context.Context) (time.Duration, bool) {
	d, ok := filterDeadlineMonoEvt(evt, ctx)
	if !ok {
		return 0, false
	}
	rem := d - monotonicNano()
	if rem <= 0 {
		return 0, true
	}
	return time.Duration(rem), true
}

// filterDeadlineMonoFromContext returns the monotonic nanosecond deadline attached to ctx.
func filterDeadlineMonoFromContext(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	d, ok := ctx.Value(filterDeadlineKey{}).(int64)
	return d, ok
}

// filterDeadlineExceeded reports whether the filter deadline in ctx has elapsed.
func filterDeadlineExceeded(ctx context.Context) bool {
	if d, ok := filterDeadlineMonoFromContext(ctx); ok {
		return monotonicNano() > d
	}
	return false
}
