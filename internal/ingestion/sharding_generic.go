//go:build !amd64

package ingestion

import (
	"hash/crc32"

	"github.com/google/uuid"
)

// crc32CastagnoliTable precomputes Castagnoli CRC for non-amd64 shard routing.
var crc32CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// crc32Castagnoli hashes campaign IDs for shard routing on non-amd64 builds.
func crc32Castagnoli(data *uuid.UUID) uint32 {
	return crc32.Checksum(data[:], crc32CastagnoliTable)
}
