package repo

import "github.com/google/uuid"

const hexChars = "0123456789abcdef"

var hexLookup [256]byte

func init() {
	for i := range hexLookup {
		hexLookup[i] = 0xff
	}
	for i := byte('0'); i <= '9'; i++ {
		hexLookup[i] = i - '0'
	}
	for i := byte('a'); i <= 'f'; i++ {
		hexLookup[i] = i - 'a' + 10
	}
	for i := byte('A'); i <= 'F'; i++ {
		hexLookup[i] = i - 'A' + 10
	}
}

// AppendUUID writes canonical UUID text into dst without allocating.
func AppendUUID(dst []byte, u uuid.UUID) []byte {
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

// ParseUUID parses a 16-byte raw or 36-byte string UUID into dst without allocations.
func ParseUUID(b []byte, dst *uuid.UUID) bool {
	if len(b) == 16 {
		copy(dst[:], b)
		return true
	}
	if len(b) != 36 {
		return false
	}
	if b[8] != '-' || b[13] != '-' || b[18] != '-' || b[23] != '-' {
		return false
	}

	decode := func(h, l byte) (byte, bool) {
		vh := hexLookup[h]
		vl := hexLookup[l]
		if vh == 0xff || vl == 0xff {
			return 0, false
		}
		return (vh << 4) | vl, true
	}

	var ok bool
	dst[0], ok = decode(b[0], b[1])
	if !ok {
		return false
	}
	dst[1], ok = decode(b[2], b[3])
	if !ok {
		return false
	}
	dst[2], ok = decode(b[4], b[5])
	if !ok {
		return false
	}
	dst[3], ok = decode(b[6], b[7])
	if !ok {
		return false
	}

	dst[4], ok = decode(b[9], b[10])
	if !ok {
		return false
	}
	dst[5], ok = decode(b[11], b[12])
	if !ok {
		return false
	}

	dst[6], ok = decode(b[14], b[15])
	if !ok {
		return false
	}
	dst[7], ok = decode(b[16], b[17])
	if !ok {
		return false
	}

	dst[8], ok = decode(b[19], b[20])
	if !ok {
		return false
	}
	dst[9], ok = decode(b[21], b[22])
	if !ok {
		return false
	}

	dst[10], ok = decode(b[24], b[25])
	if !ok {
		return false
	}
	dst[11], ok = decode(b[26], b[27])
	if !ok {
		return false
	}
	dst[12], ok = decode(b[28], b[29])
	if !ok {
		return false
	}
	dst[13], ok = decode(b[30], b[31])
	if !ok {
		return false
	}
	dst[14], ok = decode(b[32], b[33])
	if !ok {
		return false
	}
	dst[15], ok = decode(b[34], b[35])
	return ok
}
