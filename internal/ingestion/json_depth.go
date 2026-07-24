package ingestion

// JSON nesting depth caps (M14-06). Documented in one place for track + OpenRTB.
const (
	// MaxJSONDepth is the hard nest cap for parseTrackRequestJSON / skipJSONValue (default 16).
	MaxJSONDepth = 16
	// OrtbMaxJSONDepth is the OpenRTB ingress FSM nest cap (stack-bounded).
	OrtbMaxJSONDepth = 32
)

// ErrMalformed is the exported alias for malformed track/OpenRTB JSON (depth cap, syntax).
var ErrMalformed = errMalformedJSON
