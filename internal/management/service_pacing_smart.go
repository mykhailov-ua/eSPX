package management

import (
	"context"
	"time"
)

const pacingLookbackDays = 90

// fetchPacingHourWeights loads platform hour-of-day impression weights from ClickHouse (M5.6).
func (s *Service) fetchPacingHourWeights(ctx context.Context) [24]float64 {
	if s.ch == nil {
		return uniformHourWeights()
	}
	lookbackEnd := time.Now().UTC().Truncate(time.Hour)
	lookbackStart := lookbackEnd.Add(-pacingLookbackDays * 24 * time.Hour)
	_, samples, err := s.queryForecastHourlySamples(ctx, lookbackStart, lookbackEnd, nil)
	if err != nil {
		return uniformHourWeights()
	}
	return buildHourWeights(samples)
}

func uniformHourWeights() [24]float64 {
	var weights [24]float64
	for i := range weights {
		weights[i] = 1.0 / 24.0
	}
	return weights
}

// smartPacingExpectedRatio returns the cumulative daypart-weighted delivery fraction at localNow.
func smartPacingExpectedRatio(weights [24]float64, daypart []int16, localNow time.Time) float64 {
	daypartSet := make(map[int16]struct{}, len(daypart))
	for _, h := range daypart {
		daypartSet[h] = struct{}{}
	}
	useDaypart := len(daypartSet) > 0

	currentHour := localNow.Hour()
	minuteFrac := (float64(localNow.Minute()) + float64(localNow.Second())/60.0) / 60.0

	var totalWeight, elapsedWeight float64
	for h := 0; h < 24; h++ {
		if useDaypart {
			if _, ok := daypartSet[int16(h)]; !ok {
				continue
			}
		}
		w := weights[h]
		if w <= 0 {
			w = 1.0 / 24.0
		}
		totalWeight += w
		switch {
		case h < currentHour:
			elapsedWeight += w
		case h == currentHour:
			elapsedWeight += w * minuteFrac
		}
	}
	if totalWeight <= 0 {
		startOfDay := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, localNow.Location())
		elapsed := localNow.Sub(startOfDay).Seconds()
		if elapsed < 0 {
			elapsed = 0
		}
		ratio := elapsed / 86400.0
		if ratio > 1.0 {
			ratio = 1.0
		}
		return ratio
	}
	ratio := elapsedWeight / totalWeight
	if ratio > 1.0 {
		ratio = 1.0
	}
	if ratio < 0 {
		ratio = 0
	}
	return ratio
}
