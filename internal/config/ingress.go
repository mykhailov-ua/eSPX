package config

const (
	// IngressSchemaOpenRTB3 is the default /track wire format (OpenRTB 3.0 / AdCOM JSON).
	IngressSchemaOpenRTB3 = "openrtb_3"
	// IngressSchemaESPXNative keeps proprietary TrackRequest JSON + AdEvent protobuf.
	IngressSchemaESPXNative = "espx_native"
)

// IsESPXNativeIngress reports whether /track uses TrackRequest / AdEvent.
// Zero-value Config (unit tests that omit IngressSchema) defaults to espx_native;
// config.Load always sets openrtb_3 when TRACKER_INGRESS_SCHEMA is unset.
func (c *Config) IsESPXNativeIngress() bool {
	if c == nil {
		return true
	}
	switch c.IngressSchema {
	case "", IngressSchemaESPXNative:
		return true
	default:
		return false
	}
}
