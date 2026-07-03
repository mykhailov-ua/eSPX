package dashboard

import (
	"encoding/json"
	"fmt"
	"time"
)

// ChartView is the JSON contract for dashboard canvas charts.
type ChartView struct {
	Type   string        `json:"type"`
	Title  string        `json:"title"`
	Labels []string      `json:"labels"`
	Series []ChartSeries `json:"series"`
}

// ChartSeries is one line in a multi-series chart.
type ChartSeries struct {
	Name   string    `json:"name"`
	Values []float64 `json:"values"`
}

// BuildTrafficChart synthesizes a capped request-rate series for the MVP dashboard.
func BuildTrafficChart(now time.Time) ChartView {
	const points = 48
	labels := make([]string, points)
	values := make([]float64, points)
	seed := now.Unix()

	for i := 0; i < points; i++ {
		t := now.Add(time.Duration(i-points+1) * time.Second)
		labels[i] = t.Format("15:04:05")
		wobble := float64((seed+int64(i)*17)%400)
		values[i] = 1100 + wobble + float64(i%5)*12
	}

	return BuildLineChart("Ingest rate", labels, values)
}

// BuildLineChart wraps labels and values in the dashboard chart JSON contract.
func BuildLineChart(title string, labels []string, values []float64) ChartView {
	return ChartView{
		Type:   "line",
		Title:  title,
		Labels: labels,
		Series: []ChartSeries{
			{Name: "req/s", Values: values},
		},
	}
}

// ChartJSON marshals chart data for data-chart attributes.
func ChartJSON(chart ChartView) string {
	b, err := json.Marshal(chart)
	if err != nil {
		return `{"type":"line","title":"Request rate","labels":[],"series":[]}`
	}
	return string(b)
}

// ChartSummary returns an accessibility label for the chart host.
func ChartSummary(chart ChartView) string {
	if len(chart.Series) == 0 || len(chart.Series[0].Values) == 0 {
		return chart.Title + ": no data"
	}
	vals := chart.Series[0].Values
	last := vals[len(vals)-1]
	return chart.Title + ": trending around " + formatFloat(last) + " requests per second"
}

func formatFloat(v float64) string {
	return fmt.Sprintf("%.0f", v)
}
