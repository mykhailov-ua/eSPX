package log

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindActualIndexSize_CorruptPartialWrite(t *testing.T) {
	base := uint64(100)
	idx := make([]byte, 48)
	binary.BigEndian.PutUint64(idx[0:8], 100)
	binary.BigEndian.PutUint64(idx[8:16], 0)
	// Sparse zero entry after first valid row signals torn tail.
	got := findActualIndexSize(idx, base)
	assert.Equal(t, int64(16), got)
}

func TestFindActualIndexSize_BelowBaseOffset(t *testing.T) {
	base := uint64(0)
	idx := make([]byte, 32)
	binary.BigEndian.PutUint64(idx[0:8], 10)
	binary.BigEndian.PutUint64(idx[8:16], 0)
	binary.BigEndian.PutUint64(idx[16:24], 9)
	binary.BigEndian.PutUint64(idx[24:32], 16)

	got := findActualIndexSize(idx, base)
	assert.Equal(t, int64(16), got)
}

func TestFindActualIndexSize_ZeroPaddingBreak(t *testing.T) {
	base := uint64(0)
	idx := make([]byte, 48)
	binary.BigEndian.PutUint64(idx[0:8], 1)
	binary.BigEndian.PutUint64(idx[8:16], 0)
	// zeros at entry 1 signal sparse tail after crash
	got := findActualIndexSize(idx, base)
	assert.Equal(t, int64(16), got)
}
