package logcompactor

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"espx/internal/ads/pb"

	"github.com/google/uuid"
)

const maxSampleClickIDs = 5

// RollupRow is one aggregated cold-tier row inserted into ClickHouse.
type RollupRow struct {
	RollupHour         time.Time
	CampaignID         uuid.UUID
	EventType          string
	EventCount         uint64
	FraudEventCount    uint64
	BillableEventCount uint64
	SampleClickIDs     []string
	SourceSegment      string
	WarmDestSHA256     string
}

type rollupKey struct {
	campaignID uuid.UUID
	hour       time.Time
	eventType  string
}

type rollupAgg struct {
	eventCount         uint64
	fraudEventCount    uint64
	billableEventCount uint64
	sampleClickIDs     []string
}

// aggregateWarmSegment scans a warm plaintext stream and builds hourly rollups.
func aggregateWarmSegment(r io.Reader, sourceSegment, warmSHA string) ([]RollupRow, error) {
	aggs := make(map[rollupKey]*rollupAgg)
	var hdr [4]byte
	recordBuf := make([]byte, 0, 4096)
	evt := &pb.AdStreamEvent{}

	for {
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint32(hdr[:])
		if length == 0 {
			continue
		}
		if length > maxRecordBytes {
			return nil, fmt.Errorf("%w: %d bytes", ErrRecordTooLarge, length)
		}
		if int(length) > cap(recordBuf) {
			recordBuf = make([]byte, length)
		}
		record := recordBuf[:length]
		if _, err := io.ReadFull(r, record); err != nil {
			return nil, err
		}

		*evt = pb.AdStreamEvent{}
		if err := evt.UnmarshalVT(record); err != nil {
			continue
		}

		campaignID, err := campaignUUIDFromBytes(evt.CampaignId)
		if err != nil {
			continue
		}

		ts := evt.CreatedAtUnix
		if ts <= 0 {
			ts = time.Now().Unix()
		}
		hour := time.Unix(ts, 0).UTC().Truncate(time.Hour)
		eventType := string(evt.EventType)
		if eventType == "" {
			eventType = "unknown"
		}

		key := rollupKey{campaignID: campaignID, hour: hour, eventType: eventType}
		agg, ok := aggs[key]
		if !ok {
			agg = &rollupAgg{}
			aggs[key] = agg
		}
		agg.eventCount++
		if evt.FraudScore > 0 || evt.GhostEvent || len(evt.FraudReason) > 0 {
			agg.fraudEventCount++
		}
		if isAlwaysKeepEvent(evt) {
			agg.billableEventCount++
		}
		if len(agg.sampleClickIDs) < maxSampleClickIDs && len(evt.ClickId) > 0 {
			clickID := string(evt.ClickId)
			if !containsString(agg.sampleClickIDs, clickID) {
				agg.sampleClickIDs = append(agg.sampleClickIDs, clickID)
			}
		}
	}

	if len(aggs) == 0 {
		return nil, ErrEmptySegment
	}

	keys := make([]rollupKey, 0, len(aggs))
	for k := range aggs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].campaignID != keys[j].campaignID {
			return keys[i].campaignID.String() < keys[j].campaignID.String()
		}
		if !keys[i].hour.Equal(keys[j].hour) {
			return keys[i].hour.Before(keys[j].hour)
		}
		return keys[i].eventType < keys[j].eventType
	})

	rows := make([]RollupRow, 0, len(keys))
	for _, key := range keys {
		agg := aggs[key]
		rows = append(rows, RollupRow{
			RollupHour:         key.hour,
			CampaignID:         key.campaignID,
			EventType:          key.eventType,
			EventCount:         agg.eventCount,
			FraudEventCount:    agg.fraudEventCount,
			BillableEventCount: agg.billableEventCount,
			SampleClickIDs:     append([]string(nil), agg.sampleClickIDs...),
			SourceSegment:      sourceSegment,
			WarmDestSHA256:     warmSHA,
		})
	}
	return rows, nil
}

func campaignUUIDFromBytes(raw []byte) (uuid.UUID, error) {
	if len(raw) != 16 {
		return uuid.Nil, fmt.Errorf("invalid campaign_id length: %d", len(raw))
	}
	id, err := uuid.FromBytes(raw)
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func containsString(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

// ListWarm returns warm-tier segments older than the cutoff.
func (store *LocalTierStore) ListWarm(_ context.Context, olderThan time.Time) ([]TierObject, error) {
	entries, err := os.ReadDir(store.WarmDir)
	if err != nil {
		return nil, err
	}

	var objects []TierObject
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".compact.zst") || strings.HasSuffix(name, warmTmpSuffix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if !info.ModTime().Before(olderThan) {
			continue
		}
		objects = append(objects, TierObject{
			Key:     name,
			Path:    filepath.Join(store.WarmDir, name),
			ModTime: info.ModTime(),
			Size:    info.Size(),
		})
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].ModTime.Before(objects[j].ModTime)
	})
	return objects, nil
}
