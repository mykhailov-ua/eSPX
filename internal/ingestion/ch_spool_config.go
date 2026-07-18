package ingestion

// CHCfgFromConfig builds CHSpoolConfig from processor env integers.
func CHCfgFromConfig(segmentMB, maxSegments int) CHSpoolConfig {
	cfg := DefaultCHSpoolConfig()
	if segmentMB > 0 {
		cfg.SegmentSizeBytes = int64(segmentMB) * 1024 * 1024
	}
	if maxSegments > 0 {
		cfg.MaxSegments = maxSegments
	}
	return cfg
}
