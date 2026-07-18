package adminapi

import (
	"fmt"
	"math"
)

const microUnitFactor = 1_000_000

func parseMoneyMicro(micro *int64, legacy float64, hasLegacy bool, field string) (int64, error) {
	if micro != nil {
		if *micro < 0 {
			return 0, fmt.Errorf("invalid %s", field)
		}
		return *micro, nil
	}
	if hasLegacy {
		if legacy < 0 || math.IsNaN(legacy) || math.IsInf(legacy, 0) {
			return 0, fmt.Errorf("invalid %s", field)
		}
		return int64(math.Round(legacy * microUnitFactor)), nil
	}
	return 0, nil
}

func parseBudgetMicro(micro *int64, legacy float64, hasLegacy bool) (int64, error) {
	if micro != nil {
		if *micro <= 0 {
			return 0, fmt.Errorf("budget must be positive")
		}
		return *micro, nil
	}
	if hasLegacy {
		if legacy <= 0 || math.IsNaN(legacy) || math.IsInf(legacy, 0) {
			return 0, fmt.Errorf("budget must be positive")
		}
		return int64(math.Round(legacy * microUnitFactor)), nil
	}
	return 0, fmt.Errorf("budget is required")
}
