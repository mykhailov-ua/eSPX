package logcompactor

import (
	"bytes"
	"encoding/binary"
	"testing"

	"espx/internal/ads/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyCompaction_keepsLastImpressionPerClickID(t *testing.T) {
	var src bytes.Buffer
	for i := 0; i < 5; i++ {
		src.Write(encodeRecord(t, &pb.AdStreamEvent{
			EventType:     []byte("impression"),
			ClickId:       []byte("shared-click"),
			CreatedAtUnix: int64(i),
		}))
	}

	var dst bytes.Buffer
	stats, err := filterSegmentStream(bytes.NewReader(src.Bytes()), 1, &dst)
	require.NoError(t, err)
	assert.Equal(t, int64(5), stats.OriginalCount)
	assert.Equal(t, int64(1), stats.KeptCount)

	plain := dst.Bytes()
	got, err := countSegmentRecords(bytes.NewReader(plain))
	require.NoError(t, err)
	require.Equal(t, int64(1), got)

	var hdr [4]byte
	_, err = bytes.NewReader(plain).Read(hdr[:])
	require.NoError(t, err)
	length := binary.BigEndian.Uint32(hdr[:])
	record := plain[4 : 4+length]
	evt := &pb.AdStreamEvent{}
	require.NoError(t, evt.UnmarshalVT(record))
	assert.Equal(t, int64(4), evt.CreatedAtUnix)
}

func TestKeyCompaction_keepsClickWithSampledImpression(t *testing.T) {
	var src bytes.Buffer
	src.Write(encodeRecord(t, &pb.AdStreamEvent{
		EventType: []byte("impression"),
		ClickId:   []byte("billable-click"),
	}))
	src.Write(encodeRecord(t, &pb.AdStreamEvent{
		EventType: []byte("click"),
		ClickId:   []byte("billable-click"),
	}))

	var dst bytes.Buffer
	stats, err := filterSegmentStream(bytes.NewReader(src.Bytes()), 1, &dst)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.OriginalCount)
	assert.Equal(t, int64(2), stats.KeptCount)
}

func TestCompactMarkerPath(t *testing.T) {
	assert.Equal(t, "segment_foo.compact.ok", compactMarkerFileName("segment_foo.log.zst.ready"))
	assert.Equal(t, "segment_test.compact.ok", compactMarkerFileName("segment_test.log"))
}
