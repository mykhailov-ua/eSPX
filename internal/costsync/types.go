package costsync

import (
	"time"

	"github.com/google/uuid"
)

// LineType distinguishes buy-side spend from sell-side RSOC revenue.
type LineType string

const (
	LineTypeSpend   LineType = "spend"
	LineTypeRevenue LineType = "revenue"
)

// CostLine is one normalized row from a network API before persistence.
type CostLine struct {
	CustomerID  uuid.UUID
	CampaignID  uuid.UUID
	Date        time.Time
	Network     string
	PlacementID string
	AdsetID     string
	AdID        string
	LineType    LineType
	AmountMicro int64
	Currency    string
}

// Credential holds decrypted network auth material for one fetch cycle.
type Credential struct {
	CustomerID   uuid.UUID
	Network      string
	AccountID    string
	AccessToken  string
	RefreshToken string
	APIKey       string
	ExtraConfig  map[string]string
	ExpiresAt    time.Time
}
