//go:build amd64

package ads

import (
	"encoding/binary"

	"github.com/google/uuid"
)

//go:noescape
func crc32Castagnoli_asm(val1, val2 uint64) uint32

// crc32Castagnoli hashes campaign IDs with hardware CRC on amd64 for shard routing speed.
func crc32Castagnoli(data *uuid.UUID) uint32 {
	val1 := binary.LittleEndian.Uint64(data[0:8])
	val2 := binary.LittleEndian.Uint64(data[8:16])
	return crc32Castagnoli_asm(val1, val2)
}
