package licensing

// Normalized returns a copy with legacy rtb_live / ml_fraud_boost aliases applied.
func (f FeatureSet) Normalized() FeatureSet {
	out := f
	if out.OpenRTBEngine || out.RtbLive {
		out.OpenRTBEngine = true
		out.RtbLive = true
	}
	if out.MlFraudBoost {
		out.MlFraudBoost = true
	}
	return out
}

// OpenRTBEnabled reports whether the OpenRTB engine module is licensed.
func (f FeatureSet) OpenRTBEnabled() bool {
	n := f.Normalized()
	return n.OpenRTBEngine || n.RtbLive
}

// MlFraudBoostEnabled reports whether ML fraud boost scoring is licensed.
func (f FeatureSet) MlFraudBoostEnabled() bool {
	return f.Normalized().MlFraudBoost
}

// IvtMLEnabled reports whether the IVT ML detector module is licensed.
func (f FeatureSet) IvtMLEnabled() bool {
	return f.Normalized().IvtMLDetector
}

// EbpfEdgeEnabled reports whether eBPF/XDP edge filtering is licensed.
func (f FeatureSet) EbpfEdgeEnabled() bool {
	return f.Normalized().EbpfXDPEdge
}

// MultiRegionEnabled reports whether enterprise multi-cell topology is licensed.
func (f FeatureSet) MultiRegionEnabled() bool {
	return f.Normalized().MultiRegion
}
