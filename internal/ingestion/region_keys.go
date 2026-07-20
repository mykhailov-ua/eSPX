package ingestion

import (
	"github.com/google/uuid"
)

// IngressDayKey formats the per-region daily ingress counter Redis key (zero heap alloc).
func IngressDayKey(buf []byte, regionCode uint8, customerID uuid.UUID, dateStr string) []byte {
	b := append(buf[:0], "ingress:day:"...)
	if regionCode > 0 {
		b = append(b, hexByte(regionCode>>4), hexByte(regionCode&0x0f), ':')
	}
	b = appendUUID(b, customerID)
	b = append(b, ':')
	b = append(b, dateStr...)
	return b
}

func hexByte(n byte) byte {
	if n < 10 {
		return '0' + n
	}
	return 'a' + (n - 10)
}
