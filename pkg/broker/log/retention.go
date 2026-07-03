package log

import (
	"fmt"
	"os"
	"time"
)

// RetentionPolicy bounds sealed segment lifetime by age and total on-disk bytes.
// FloorOffset is reserved for consumer commit floors (0 = age/bytes + safety only).
type RetentionPolicy struct {
	MaxAge         time.Duration
	MaxBytes       int64
	FloorOffset    uint64
	SafetyMessages uint64
}

// Enabled reports whether any retention limit is configured.
func (p RetentionPolicy) Enabled() bool {
	return p.MaxAge > 0 || p.MaxBytes > 0
}

// RetentionResult summarizes one ApplyRetention pass.
type RetentionResult struct {
	DeletedSegments  int
	BytesFreed       int64
	OldestSegmentAge time.Duration
	TotalBytes       int64
}

// segmentMeta is a sealed-segment view used for retention decisions.
type segmentMeta struct {
	seg        *Segment
	baseOffset uint64
	highOffset uint64
	bytes      int64
	modTime    time.Time
}

// BaseOffset returns the first message offset stored in the segment.
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

// OnDiskBytes returns combined log and index file sizes for retention accounting.
func (s *Segment) OnDiskBytes() (int64, error) {
	logInfo, err := s.logFile.Stat()
	if err != nil {
		return 0, err
	}
	idxInfo, err := s.indexFile.Stat()
	if err != nil {
		return 0, err
	}
	return logInfo.Size() + idxInfo.Size(), nil
}

// ModTime returns the log file modification time used for age-based retention.
func (s *Segment) ModTime() (time.Time, error) {
	info, err := s.logFile.Stat()
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

// ApplyRetention deletes sealed segments that satisfy age/byte limits and stay below the safety floor.
func (p *PartitionLog) ApplyRetention(policy RetentionPolicy) (RetentionResult, error) {
	var result RetentionResult
	if !policy.Enabled() {
		return result, nil
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	snap := p.snap.Load()
	if snap == nil || len(snap.segments) == 0 {
		return result, nil
	}

	now := time.Now()
	metas, totalBytes, oldestAge, err := p.buildSegmentMetas(snap, now)
	if err != nil {
		return result, err
	}
	result.TotalBytes = totalBytes
	result.OldestSegmentAge = oldestAge

	if len(metas) == 0 {
		return result, nil
	}

	safeOffset, enforceOffset := retentionSafeOffset(p.nextOffset, policy)
	toDelete := selectRetentionVictims(metas, policy, safeOffset, enforceOffset, now, totalBytes)
	if len(toDelete) == 0 {
		return result, nil
	}

	deleteSet := make(map[uint64]*Segment, len(toDelete))
	for _, m := range toDelete {
		deleteSet[m.baseOffset] = m.seg
		result.BytesFreed += m.bytes
	}

	newSegments := make([]*Segment, 0, len(snap.segments)-len(toDelete))
	for _, seg := range snap.segments {
		if _, drop := deleteSet[seg.baseOffset]; drop {
			if err := removeSegmentFiles(seg); err != nil {
				return result, err
			}
			result.DeletedSegments++
			continue
		}
		newSegments = append(newSegments, seg)
	}

	if len(newSegments) == 0 {
		return result, fmt.Errorf("retention would remove all segments")
	}

	activeBase := snap.activeSeg.baseOffset
	active := newSegments[len(newSegments)-1]
	for _, seg := range newSegments {
		if seg.baseOffset == activeBase {
			active = seg
			break
		}
	}

	p.snap.Store(&segmentSnapshot{
		segments:  newSegments,
		activeSeg: active,
	})

	return result, nil
}

func (p *PartitionLog) buildSegmentMetas(snap *segmentSnapshot, now time.Time) ([]segmentMeta, int64, time.Duration, error) {
	var (
		totalBytes int64
		oldestAge  time.Duration
		hasOldest  bool
	)
	metas := make([]segmentMeta, 0, len(snap.segments)-1)

	for i, seg := range snap.segments {
		if seg == snap.activeSeg {
			continue
		}

		bytes, err := seg.OnDiskBytes()
		if err != nil {
			return nil, 0, 0, err
		}
		totalBytes += bytes

		modTime, err := seg.ModTime()
		if err != nil {
			return nil, 0, 0, err
		}
		age := now.Sub(modTime)
		if !hasOldest || age > oldestAge {
			oldestAge = age
			hasOldest = true
		}

		var nextBase uint64
		if i+1 < len(snap.segments) {
			nextBase = snap.segments[i+1].baseOffset
		} else {
			nextBase = p.nextOffset
		}

		highOffset := segmentHighOffset(seg.baseOffset, nextBase, p.nextOffset)
		metas = append(metas, segmentMeta{
			seg:        seg,
			baseOffset: seg.baseOffset,
			highOffset: highOffset,
			bytes:      bytes,
			modTime:    modTime,
		})
	}

	activeBytes, err := snap.activeSeg.OnDiskBytes()
	if err != nil {
		return nil, 0, 0, err
	}
	totalBytes += activeBytes

	activeMod, err := snap.activeSeg.ModTime()
	if err != nil {
		return nil, 0, 0, err
	}
	activeAge := now.Sub(activeMod)
	if !hasOldest || activeAge > oldestAge {
		oldestAge = activeAge
	}

	return metas, totalBytes, oldestAge, nil
}

func segmentHighOffset(baseOffset, nextBaseOffset, headOffset uint64) uint64 {
	if nextBaseOffset > baseOffset {
		if nextBaseOffset == 0 {
			return baseOffset
		}
		return nextBaseOffset - 1
	}
	if headOffset > baseOffset {
		return headOffset - 1
	}
	return baseOffset
}

func retentionSafeOffset(headOffset uint64, policy RetentionPolicy) (safe uint64, enforce bool) {
	enforce = policy.SafetyMessages > 0 || policy.FloorOffset > 0
	if policy.SafetyMessages > 0 && headOffset > policy.SafetyMessages {
		safe = headOffset - policy.SafetyMessages
	}
	if policy.FloorOffset > safe {
		safe = policy.FloorOffset
	}
	return safe, enforce
}

func selectRetentionVictims(metas []segmentMeta, policy RetentionPolicy, safeOffset uint64, enforceOffset bool, now time.Time, totalBytes int64) []segmentMeta {
	if len(metas) == 0 {
		return nil
	}

	ageCutoff := time.Time{}
	if policy.MaxAge > 0 {
		ageCutoff = now.Add(-policy.MaxAge)
	}

	candidates := make([]segmentMeta, 0, len(metas))
	for _, m := range metas {
		if enforceOffset && m.highOffset >= safeOffset {
			continue
		}
		candidates = append(candidates, m)
	}
	if len(candidates) == 0 {
		return nil
	}

	selected := make(map[uint64]struct{})

	if policy.MaxAge > 0 {
		for _, m := range candidates {
			if m.modTime.Before(ageCutoff) {
				selected[m.baseOffset] = struct{}{}
			}
		}
	}

	if policy.MaxBytes > 0 && totalBytes > policy.MaxBytes {
		remaining := totalBytes
		for _, m := range candidates {
			if _, ok := selected[m.baseOffset]; ok {
				remaining -= m.bytes
			}
		}
		for _, m := range candidates {
			if remaining <= policy.MaxBytes {
				break
			}
			if _, ok := selected[m.baseOffset]; ok {
				continue
			}
			selected[m.baseOffset] = struct{}{}
			remaining -= m.bytes
		}
	}

	if len(selected) == 0 {
		return nil
	}

	out := make([]segmentMeta, 0, len(selected))
	for _, m := range candidates {
		if _, ok := selected[m.baseOffset]; ok {
			out = append(out, m)
		}
	}
	return out
}

func removeSegmentFiles(seg *Segment) error {
	_ = seg.Close()
	var errs []error
	if err := os.Remove(seg.logPath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if err := os.Remove(seg.indexPath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
