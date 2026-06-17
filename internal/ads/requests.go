package ads

import (
	"encoding/json"
	"github.com/google/uuid"
	jlexer "github.com/mailru/easyjson/jlexer"
)

// TrackRequest is the JSON track payload parsed without reflection on the hot path.
//
//easyjson:skip
type TrackRequest struct {
	CampaignID uuid.UUID       `json:"campaign_id"`
	UserID     string          `json:"user_id"`
	Type       string          `json:"type"`
	ClickID    string          `json:"click_id"`
	Payload    json.RawMessage `json:"payload"`
}

// UnmarshalEasyJSON parses track JSON with zero-copy string fields where safe.
func (v *TrackRequest) UnmarshalEasyJSON(in *jlexer.Lexer) {
	if in.IsNull() {
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "campaign_id":
			if data := in.UnsafeBytes(); in.Ok() {
				_ = v.CampaignID.UnmarshalText(data)
			}
		case "user_id":
			v.UserID = in.UnsafeString()
		case "type":
			v.Type = in.UnsafeString()
		case "click_id":
			v.ClickID = in.UnsafeString()
		case "payload":
			v.Payload = in.Raw()
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
}

// UnmarshalJSON delegates to the easyjson parser for allocation-free decoding.
func (v *TrackRequest) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	v.UnmarshalEasyJSON(&r)
	return r.Error()
}
