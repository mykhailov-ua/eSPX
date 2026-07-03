package dashboard

import "time"

// Page is the view model for the main admin dashboard layout.
type Page struct {
	CSRF       string
	UserEmail  string
	UserRole   string
	PollEvery  string
	Snapshot   Snapshot
}

// Snapshot is a cached metrics view model for dashboard panels.
type Snapshot struct {
	RequestsPerSec  string
	ActiveCampaigns string
	ErrorRate       string
	PendingOutbox   string
	UpdatedAt       time.Time
	RecentEvents    []EventRow
	TrafficChart    ChartView
}

// EventRow is a single row in the recent activity table.
type EventRow struct {
	Time   string
	Type   string
	Detail string
}
