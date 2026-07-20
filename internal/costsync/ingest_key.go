package costsync

import (
	"time"

	"github.com/google/uuid"
)

const hexChars = "0123456789abcdef"

// ingestKeyStackCap fits two UUIDs, ISO date, separators, and typical network/placement ids.
const ingestKeyStackCap = 192

// IngestKey builds a deterministic idempotency key for campaign_costs.ingest_key.
// One heap allocation: the returned string (stack buffer for assembly).
func IngestKey(customerID, campaignID uuid.UUID, date time.Time, network, placementID string, lineType LineType) string {
	var buf [ingestKeyStackCap]byte
	b := buf[:0]
	b = appendUUID(b, customerID)
	b = append(b, '|')
	b = appendUUID(b, campaignID)
	b = append(b, '|')
	b = appendDateISO(b, date.UTC())
	b = append(b, '|')
	b = append(b, network...)
	b = append(b, '|')
	b = append(b, placementID...)
	b = append(b, '|')
	b = append(b, string(lineType)...)
	return string(b)
}

func appendUUID(dst []byte, u uuid.UUID) []byte {
	return append(dst,
		hexChars[u[0]>>4], hexChars[u[0]&0xf],
		hexChars[u[1]>>4], hexChars[u[1]&0xf],
		hexChars[u[2]>>4], hexChars[u[2]&0xf],
		hexChars[u[3]>>4], hexChars[u[3]&0xf],
		'-',
		hexChars[u[4]>>4], hexChars[u[4]&0xf],
		hexChars[u[5]>>4], hexChars[u[5]&0xf],
		'-',
		hexChars[u[6]>>4], hexChars[u[6]&0xf],
		hexChars[u[7]>>4], hexChars[u[7]&0xf],
		'-',
		hexChars[u[8]>>4], hexChars[u[8]&0xf],
		hexChars[u[9]>>4], hexChars[u[9]&0xf],
		'-',
		hexChars[u[10]>>4], hexChars[u[10]&0xf],
		hexChars[u[11]>>4], hexChars[u[11]&0xf],
		hexChars[u[12]>>4], hexChars[u[12]&0xf],
		hexChars[u[13]>>4], hexChars[u[13]&0xf],
		hexChars[u[14]>>4], hexChars[u[14]&0xf],
		hexChars[u[15]>>4], hexChars[u[15]&0xf],
	)
}

func appendDateISO(dst []byte, t time.Time) []byte {
	y, m, d := t.Date()
	dst = appendDigit2(dst, y/100)
	dst = appendDigit2(dst, y%100)
	dst = append(dst, '-')
	dst = appendDigit2(dst, int(m))
	dst = append(dst, '-')
	dst = appendDigit2(dst, d)
	return dst
}

func appendDigit2(dst []byte, n int) []byte {
	if n < 10 {
		return append(dst, '0', byte('0'+n))
	}
	return append(dst, byte('0'+n/10), byte('0'+n%10))
}
