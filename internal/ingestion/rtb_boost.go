package ingestion

// BoostPPMFromUint8 maps ML score boost (0-100) to ranking PPM multiplier (1.0 + boost%).
func BoostPPMFromUint8(boost uint8) uint32 {
	if boost == 0 {
		return CTRPPMUnit
	}
	return CTRPPMUnit + uint32(boost)*10_000
}
