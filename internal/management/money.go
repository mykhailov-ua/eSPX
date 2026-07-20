package management

import (
	"fmt"

	"espx/pkg/money"
)

// parseMoneyMicro resolves a monetary amount preferring explicit micro fields over legacy floats.
func parseMoneyMicro(micro *int64, legacy float64, hasLegacy bool, field string) (int64, error) {
	if micro != nil {
		if *micro < 0 {
			return 0, errValidation(fmt.Sprintf("invalid %s", field))
		}
		return *micro, nil
	}
	if hasLegacy {
		v, err := money.LegacyFloatToMicro(legacy)
		if err != nil {
			return 0, errValidation(fmt.Sprintf("invalid %s", field))
		}
		return v, nil
	}
	return 0, nil
}

// parseBudgetMicro resolves budget fields that may arrive as budget_micro or budget_limit dollars.
func parseBudgetMicro(micro *int64, legacy float64, hasLegacy bool) (int64, error) {
	if micro != nil {
		if *micro <= 0 {
			return 0, errValidation("budget must be positive")
		}
		return *micro, nil
	}
	if hasLegacy {
		v, err := money.LegacyFloatToMicro(legacy)
		if err != nil || v <= 0 {
			return 0, errValidation("budget must be positive")
		}
		return v, nil
	}
	return 0, errValidation("budget is required")
}

// optionalBudgetMicro resolves an optional override budget from micro or legacy fields.
func optionalBudgetMicro(micro *int64, legacy *float64) (*int64, error) {
	if micro != nil {
		if *micro <= 0 {
			return nil, errValidation("budget must be positive")
		}
		v := *micro
		return &v, nil
	}
	if legacy != nil {
		v, err := money.LegacyFloatToMicro(*legacy)
		if err != nil || v <= 0 {
			return nil, errValidation("budget must be positive")
		}
		return &v, nil
	}
	return nil, nil
}
