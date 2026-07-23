// Package money provides integer micro-unit helpers for ledger-safe arithmetic.
// Use int64 micro-units (10^6 per currency unit) for all internal money operations.
// float64 is allowed only at external API egress via APIValueFloat.
package money

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// MicroUnit is the number of micro-units per one currency unit (e.g. one US dollar).
const MicroUnit = int64(1_000_000)

// ErrInvalidAmount indicates a negative, NaN, or infinite monetary value.
var ErrInvalidAmount = errors.New("invalid money amount")

// ParseDecimal parses a decimal string (e.g. "12.34", "-0.5") into micro-units.
func ParseDecimal(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = strings.TrimPrefix(s, "-")
	}
	if s == "" {
		return 0, ErrInvalidAmount
	}

	var whole, frac int64
	if strings.Contains(s, ".") {
		parts := strings.SplitN(s, ".", 2)
		if _, err := fmt.Sscanf(parts[0], "%d", &whole); err != nil {
			return 0, ErrInvalidAmount
		}
		fracStr := parts[1]
		if len(fracStr) > 6 {
			fracStr = fracStr[:6]
		}
		for len(fracStr) < 6 {
			fracStr += "0"
		}
		if _, err := fmt.Sscanf(fracStr, "%d", &frac); err != nil {
			return 0, ErrInvalidAmount
		}
	} else {
		if _, err := fmt.Sscanf(s, "%d", &whole); err != nil {
			return 0, ErrInvalidAmount
		}
	}
	if whole < 0 || frac < 0 {
		return 0, ErrInvalidAmount
	}
	out := whole*MicroUnit + frac
	if neg {
		out = -out
	}
	return out, nil
}

// LegacyFloatToMicro converts a legacy dollar float from JSON/API input via decimal round-trip.
func LegacyFloatToMicro(v float64) (int64, error) {
	if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, ErrInvalidAmount
	}
	return ParseDecimal(strconv.FormatFloat(v, 'f', 6, 64))
}

// JSONAmountToMicro converts an external JSON number field to micro-units.
func JSONAmountToMicro(v float64) (int64, error) {
	return LegacyFloatToMicro(v)
}

// PercentBps returns amountMicro * bps / 10000 (bps = basis points; 100 bps = 1%).
func PercentBps(amountMicro, bps int64) int64 {
	if amountMicro <= 0 || bps <= 0 {
		return 0
	}
	return amountMicro * bps / 10000
}

// PercentFromFloat returns amountMicro * percent / 100 using rounded hundredths.
func PercentFromFloat(amountMicro int64, percent float64) int64 {
	if amountMicro <= 0 || percent <= 0 {
		return 0
	}
	return PercentHundredths(amountMicro, int64(math.Round(percent*100)))
}

// PercentHundredths returns amountMicro * hundredths / 10000 (hundredths = percent * 100, e.g. 250 = 2.5%).
func PercentHundredths(amountMicro, hundredths int64) int64 {
	if amountMicro <= 0 || hundredths <= 0 {
		return 0
	}
	return amountMicro * hundredths / 10000
}

// ScalePPM returns amountMicro * ppm / 1_000_000 (parts per million).
func ScalePPM(amountMicro, ppm int64) int64 {
	if amountMicro <= 0 || ppm <= 0 {
		return 0
	}
	return amountMicro * ppm / 1_000_000
}

// MulMicro returns a * b when a is a per-unit micro amount and b is a whole-unit count.
func MulMicro(perUnitMicro, count int64) int64 {
	if perUnitMicro <= 0 || count <= 0 {
		return 0
	}
	return perUnitMicro * count
}

// FormatFixed2 renders micro-units with exactly two fractional digits for API display.
func FormatFixed2(micro int64) string {
	neg := micro < 0
	if neg {
		micro = -micro
	}
	whole := micro / MicroUnit
	frac := (micro % MicroUnit) / 10_000
	sign := ""
	if neg {
		sign = "-"
	}
	return fmt.Sprintf("%s%d.%02d", sign, whole, frac)
}

// FormatDecimal renders micro-units as a decimal string without float64 arithmetic.
func FormatDecimal(micro int64) string {
	neg := micro < 0
	if neg {
		micro = -micro
	}
	whole := micro / MicroUnit
	frac := micro % MicroUnit
	if frac == 0 {
		if neg {
			return "-" + strconv.FormatInt(whole, 10)
		}
		return strconv.FormatInt(whole, 10)
	}
	fracStr := strconv.FormatInt(frac, 10)
	for len(fracStr) < 6 {
		fracStr = "0" + fracStr
	}
	fracStr = strings.TrimRight(fracStr, "0")
	sign := ""
	if neg {
		sign = "-"
	}
	return sign + strconv.FormatInt(whole, 10) + "." + fracStr
}

// APIValueFloat converts micro-units to float64 for external APIs that require JSON numbers.
// Use only at egress boundaries - never for ledger math.
func APIValueFloat(micro int64) float64 {
	return float64(micro) / float64(MicroUnit)
}
