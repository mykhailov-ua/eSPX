package logcompactor

import (
	"bytes"
	"encoding/binary"
	"testing"

	"espx/internal/ingestion/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encodeRecord(t *testing.T, evt *pb.AdStreamEvent) []byte {
	t.Helper()
	payload, err := evt.MarshalVT()
	require.NoError(t, err)
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	buf.Write(lenBuf[:])
	buf.Write(payload)
	return buf.Bytes()
}

func TestFilterSegment_downsamplesImpressions(t *testing.T) {
	var src bytes.Buffer
	for i := 0; i < 2000; i++ {
		evt := &pb.AdStreamEvent{
			EventType: []byte("impression"),
			ClickId:   []byte("impression-" + string(rune(i))),
		}
		src.Write(encodeRecord(t, evt))
	}
	src.Write(encodeRecord(t, &pb.AdStreamEvent{
		EventType: []byte("click"),
		ClickId:   []byte("billable-click"),
	}))

	var dst bytes.Buffer
	stats, err := filterSegment(src.Bytes(), 1000, &dst)
	require.NoError(t, err)
	assert.Equal(t, int64(2001), stats.OriginalCount)
	assert.InDelta(t, float64(2), float64(stats.KeptCount), 2) // ~2 impressions + 1 click
	assert.GreaterOrEqual(t, stats.KeptCount, int64(1))        // click always kept
}

func TestFilterSegment_emptySegment(t *testing.T) {
	_, err := filterSegment(nil, 1000, &bytes.Buffer{})
	assert.ErrorIs(t, err, ErrEmptySegment)
}

func TestWarmDestKey(t *testing.T) {
	assert.Equal(t, "segment_20260704.compact.zst", warmDestKey("segment_20260704.log.zst.ready"))
	assert.Equal(t, "segment_20260704.compact.zst", warmDestKey("segment_20260704.log"))
}
