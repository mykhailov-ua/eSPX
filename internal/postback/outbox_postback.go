package postback

import (
	"encoding/json"

	"espx/pkg/money"

	"github.com/google/uuid"
)

type PostbackPayload struct {
	CustomerID  uuid.UUID `json:"customer_id"`
	CampaignID  uuid.UUID `json:"campaign_id"`
	ClickID     string    `json:"click_id"`
	EventType   string    `json:"event_type"`
	PayoutMicro int64     `json:"payout_micro"`
	TxID        string    `json:"tx_id"`
	SubID1      string    `json:"subid1"`
	Param10     string    `json:"param10"`
	Email       string    `json:"email"`
	Phone       string    `json:"phone"`
	FBCLID      string    `json:"fbclid"`
	GCLID       string    `json:"gclid"`
	TTCLID      string    `json:"ttclid"`
}

// UnmarshalJSON accepts payout_micro or legacy payout (dollar float).
func (p *PostbackPayload) UnmarshalJSON(data []byte) error {
	type payloadAlias PostbackPayload
	aux := struct {
		payloadAlias
		PayoutLegacy *float64 `json:"payout"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*p = PostbackPayload(aux.payloadAlias)
	if p.PayoutMicro == 0 && aux.PayoutLegacy != nil {
		micro, err := money.LegacyFloatToMicro(*aux.PayoutLegacy)
		if err != nil {
			return err
		}
		p.PayoutMicro = micro
	}
	return nil
}

// PayoutDollarsAPI returns the payout as a float for external network APIs (egress only).
func (p *PostbackPayload) PayoutDollarsAPI() float64 {
	return money.APIValueFloat(p.PayoutMicro)
}
