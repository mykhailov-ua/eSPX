package licensing

// VolumeBand is the commercial prepaid volume tier (ESPX-LP-2026-V1).
type VolumeBand string

const (
	VolumeBandSmall  VolumeBand = "S"
	VolumeBandMedium VolumeBand = "M"
	VolumeBandLarge  VolumeBand = "L"
)

// BillableCategory classifies events for weighted PU metering.
type BillableCategory uint8

const (
	BillableAccepted BillableCategory = iota
	BillableDedupReject
	BillableEbpfDrop
)

const (
	weightAccepted    = 1.0
	weightDedupReject = 0.1
	weightEbpfDrop    = 0.0
)

// BandIncludedEvents is the prepaid monthly billable-event ceiling per band.
var BandIncludedEvents = map[VolumeBand]uint64{
	VolumeBandSmall:  10_000_000_000,
	VolumeBandMedium: 50_000_000_000,
	VolumeBandLarge:  100_000_000_000,
}

// BasePU is κ_base per volume band.
var BasePU = map[VolumeBand]int{
	VolumeBandSmall:  100,
	VolumeBandMedium: 250,
	VolumeBandLarge:  500,
}

// ModulePU holds κ_module coefficients per band.
type ModulePU struct {
	OpenRTBEngine int
	EbpfXDPEdge   int
	IvtMLDetector int
	MlFraudBoost  int
}

// ModuleCoefficients maps subsystem flags to PU add-ons per band.
var ModuleCoefficients = map[VolumeBand]ModulePU{
	VolumeBandSmall:  {OpenRTBEngine: 50, EbpfXDPEdge: 40, IvtMLDetector: 40, MlFraudBoost: 30},
	VolumeBandMedium: {OpenRTBEngine: 120, EbpfXDPEdge: 100, IvtMLDetector: 80, MlFraudBoost: 60},
	VolumeBandLarge:  {OpenRTBEngine: 250, EbpfXDPEdge: 200, IvtMLDetector: 150, MlFraudBoost: 100},
}

// BillableWeight returns the PU multiplier for a billable category.
func BillableWeight(cat BillableCategory) float64 {
	switch cat {
	case BillableAccepted:
		return weightAccepted
	case BillableDedupReject:
		return weightDedupReject
	case BillableEbpfDrop:
		return weightEbpfDrop
	default:
		return weightAccepted
	}
}

// BillableWeightPermille returns the PU multiplier scaled by 1000 (1.0 → 1000, 0.1 → 100).
func BillableWeightPermille(cat BillableCategory) int64 {
	switch cat {
	case BillableAccepted:
		return 1000
	case BillableDedupReject:
		return 100
	case BillableEbpfDrop:
		return 0
	default:
		return 1000
	}
}

// ClassifyEventType maps ClickHouse audit event_type strings to billable categories.
func ClassifyEventType(eventType string) BillableCategory {
	switch eventType {
	case "duplicate", "dedup", "dedup_reject", "freq", "fcap", "rate_limit":
		return BillableDedupReject
	case "ebpf_drop", "l3_blocklist", "tls_blocklist", "xdp_drop":
		return BillableEbpfDrop
	default:
		return BillableAccepted
	}
}

// WeightedBillableUnits computes Σ(count[category] × weight[category]).
func WeightedBillableUnits(counts map[BillableCategory]uint64) float64 {
	var total float64
	for cat, n := range counts {
		total += float64(n) * BillableWeight(cat)
	}
	return total
}

// MonthlyPU returns abstract pricing units for a deployment license band + enabled modules.
func MonthlyPU(band VolumeBand, features FeatureSet) int {
	if band == "" {
		band = VolumeBandSmall
	}
	pu := BasePU[band]
	mods := ModuleCoefficients[band]
	features = features.Normalized()
	if features.OpenRTBEnabled() {
		pu += mods.OpenRTBEngine
	}
	if features.EbpfXDPEdge {
		pu += mods.EbpfXDPEdge
	}
	if features.IvtMLDetector {
		pu += mods.IvtMLDetector
	}
	if features.MlFraudBoostEnabled() {
		pu += mods.MlFraudBoost
	}
	return pu
}

// ParseVolumeBand normalizes JWT volume_band values.
func ParseVolumeBand(raw string) VolumeBand {
	switch VolumeBand(raw) {
	case VolumeBandSmall, VolumeBandMedium, VolumeBandLarge:
		return VolumeBand(raw)
	default:
		return VolumeBandSmall
	}
}
