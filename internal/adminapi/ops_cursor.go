package adminapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

type fanOutCursorState struct {
	Sources map[string]string `json:"sources"`
}

// EncodeFanOutCursor serializes cursor state to opaque base64 JSON.
func EncodeFanOutCursor(state map[string]string) (string, error) {
	if len(state) == 0 {
		return "", nil
	}
	raw, err := json.Marshal(fanOutCursorState{Sources: state})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeFanOutCursor parses an opaque cursor query parameter.
func DecodeFanOutCursor(raw string) (map[string]string, error) {
	if raw == "" {
		return map[string]string{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	var state fanOutCursorState
	if err := json.Unmarshal(decoded, &state); err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	if state.Sources == nil {
		state.Sources = map[string]string{}
	}
	return state.Sources, nil
}
