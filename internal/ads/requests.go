package ads

import (
	"encoding/json"
	"github.com/google/uuid"
)

//easyjson:json
type TrackRequest struct {
	CampaignID uuid.UUID       `json:"campaign_id"`
	UserID     string          `json:"user_id"`
	Type       string          `json:"type"`
	ClickID    string          `json:"click_id"`
	Payload    json.RawMessage `json:"payload"`
}
