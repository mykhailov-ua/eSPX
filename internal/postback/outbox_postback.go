package postback

import "github.com/google/uuid"

type PostbackPayload struct {
	CustomerID uuid.UUID `json:"customer_id"`
	CampaignID uuid.UUID `json:"campaign_id"`
	ClickID    string    `json:"click_id"`
	EventType  string    `json:"event_type"`
	Payout     float64   `json:"payout"`
	TxID       string    `json:"tx_id"`
	SubID1     string    `json:"subid1"`
	Param10    string    `json:"param10"`
	Email      string    `json:"email"`
	Phone      string    `json:"phone"`
	FBCLID     string    `json:"fbclid"`
	GCLID      string    `json:"gclid"`
	TTCLID     string    `json:"ttclid"`
}
