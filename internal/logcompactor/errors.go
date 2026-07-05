package logcompactor

import "errors"

var (
	ErrCloudStoreNotConfigured = errors.New("cloud tier store is not configured")
	ErrCloudConfigIncomplete   = errors.New("LOG_COMPACTOR_S3_BUCKET and LOG_COMPACTOR_S3_REGION are required for s3 backend")
	ErrCheckpointCorrupt       = errors.New("compactor checkpoint file is corrupt")
	ErrSegmentTooShort         = errors.New("segment payload shorter than length prefix")
	ErrEmptySegment            = errors.New("segment contains no records")
	ErrRecordTooLarge          = errors.New("segment record exceeds max size")
	ErrVerifyRecordCount       = errors.New("warm segment record count mismatch")
	ErrHotSegmentNotFound      = errors.New("hot segment not found")
	ErrCompactingInUse         = errors.New("segment is already being compacted")
	ErrCompactionFailures      = errors.New("one or more segments failed compaction")
	ErrColdRollupFailures      = errors.New("one or more warm segments failed cold rollup")
)
