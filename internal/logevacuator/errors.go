package logevacuator

import "errors"

var (
	ErrBucketRequired    = errors.New("object storage bucket is required")
	ErrRegionRequired    = errors.New("AWS region is required")
	ErrDigestMismatch    = errors.New("uploaded object digest mismatch")
	ErrETagMismatch      = errors.New("uploaded object ETag mismatch")
	ErrEvacuatingInUse   = errors.New("segment is already being evacuated")
	ErrNotReadySegment   = errors.New("segment is not in ready state")
	ErrCheckpointCorrupt = errors.New("checkpoint file is corrupt")
)
