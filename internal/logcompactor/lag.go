package logcompactor

import (
	"context"
	"time"
)

func refreshHotLag(ctx context.Context, store TierStore, checkpoint checkpointStore) {
	objects, err := store.ListHot(ctx, time.Now())
	if err != nil {
		return
	}

	var oldest time.Time
	pending := 0
	for _, obj := range objects {
		digest, err := computeFileDigest(obj.Path)
		if err != nil {
			continue
		}
		if checkpoint.IsCompacted(obj.Key, digest.SHA256) {
			continue
		}
		pending++
		if oldest.IsZero() || obj.ModTime.Before(oldest) {
			oldest = obj.ModTime
		}
	}

	hotPendingTotal.Set(float64(pending))
	if oldest.IsZero() {
		hotLagSeconds.Set(0)
		return
	}
	hotLagSeconds.Set(time.Since(oldest).Seconds())
}

func refreshColdLag(objects []TierObject, checkpoint *CheckpointStore) {
	var oldest time.Time
	pending := 0
	for _, obj := range objects {
		digest, err := computeFileDigest(obj.Path)
		if err != nil {
			continue
		}
		if checkpoint.IsCompacted(obj.Key, digest.SHA256) {
			continue
		}
		pending++
		if oldest.IsZero() || obj.ModTime.Before(oldest) {
			oldest = obj.ModTime
		}
	}

	warmPendingTotal.Set(float64(pending))
	if oldest.IsZero() {
		coldLagSeconds.Set(0)
		return
	}
	coldLagSeconds.Set(time.Since(oldest).Seconds())
}
