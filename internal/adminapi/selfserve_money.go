package adminapi

import (
	"fmt"

	"espx/pkg/money"
)

func parseMoneyMicro(micro *int64, legacy float64, hasLegacy bool, field string) (int64, error) {
	if micro != nil {
		if *micro < 0 {
			return 0, fmt.Errorf("invalid %s", field)
		}
		return *micro, nil
	}
	if hasLegacy {
		v, err := money.LegacyFloatToMicro(legacy)
		if err != nil {
			return 0, fmt.Errorf("invalid %s", field)
		}
		return v, nil
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
		v, err := money.LegacyFloatToMicro(legacy)
		if err != nil || v <= 0 {
			return 0, fmt.Errorf("budget must be positive")
		}
		return v, nil
	}
	return 0, fmt.Errorf("budget is required")
}
