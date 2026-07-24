package ingestion

import (
	"github.com/google/uuid"
)

// TrackRequest is the JSON track payload parsed without reflection on the hot path.
type TrackRequest struct {
	CampaignID  uuid.UUID
	UserID      string
	Type        string
	ClickID     string
	PlacementID string
	Payload     []byte
	ortbSlot    *openRTBScratchSlot // pooled OpenRTB parse cache; transferred to Event.Scratch on ingest
}

// Reset clears fields before reuse; Payload is nil'd to drop input-buffer references.
func (v *TrackRequest) Reset() {
	v.resetForParse()
	v.Payload = nil
	if v.ortbSlot != nil {
		releaseOpenRTBScratchSlot(v.ortbSlot)
		v.ortbSlot = nil
	}
}

// resetForParse clears scalar fields without dropping Payload or the OpenRTB parse cache.
func (v *TrackRequest) resetForParse() {
	v.CampaignID = uuid.Nil
	v.UserID = ""
	v.Type = ""
	v.ClickID = ""
	v.PlacementID = ""
}

// UnmarshalJSON decodes track JSON for encoding/json compatibility on cold paths.
func (v *TrackRequest) UnmarshalJSON(data []byte) error {
	return ParseTrackRequestJSON(v, data)
}

// appendJSONString appends a JSON-escaped string to dst.
func appendJSONString(dst []byte, s []byte) []byte {
	dst = append(dst, '"')
	for _, b := range s {
		switch b {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, b)
		}
	}
	dst = append(dst, '"')
	return dst
}

// marshalExtra serializes parallel extra key/value slices to JSON without reflection.
func marshalExtra(dst []byte, keys, values [][]byte) []byte {
	dst = dst[:0]
	dst = append(dst, '{')
	for i, key := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, key)
		dst = append(dst, ':')
		if i < len(values) {
			dst = appendJSONString(dst, values[i])
		} else {
			dst = append(dst, '"', '"')
		}
	}
	dst = append(dst, '}')
	return dst
}
