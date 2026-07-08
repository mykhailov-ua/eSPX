package logcompactor

const readySuffix = ".log.zst.ready"

// compactStats tracks how many records were scanned vs kept during one compaction pass.
type compactStats struct {
	OriginalCount int64
	KeptCount     int64
}
