package ingestion

// http1_chunked.go — chunked Transfer-Encoding body decoder for M5-B3.

// teValueOnlyChunked reports whether Transfer-Encoding is exactly "chunked" (no gzip, etc.).
func teValueOnlyChunked(val []byte) bool {
	if !teValueHasChunked(val) {
		return false
	}
	i := 0
	found := false
	for i < len(val) {
		for i < len(val) && (val[i] == ' ' || val[i] == '\t' || val[i] == ',') {
			i++
		}
		if i >= len(val) {
			break
		}
		start := i
		for i < len(val) && val[i] != ',' {
			i++
		}
		token := trimHTTPVal(val[start:i])
		if len(token) == 0 {
			continue
		}
		if len(token) == 7 &&
			foldKeyU32(token, 0) == 0x6e756863 &&
			httpFold[token[4]] == 'k' &&
			httpFold[token[5]] == 'e' &&
			httpFold[token[6]] == 'd' {
			if found {
				return false
			}
			found = true
			continue
		}
		return false
	}
	return found
}

// parseHTTP1ChunkedBody decodes a chunked body starting at off.
// scratch assembles non-contiguous multi-chunk bodies (reused slice returned via scratchOut).
func parseHTTP1ChunkedBody(data []byte, off int, maxBody int64, scratch []byte) (consumed int, body []byte, contentLen int, scratchOut []byte, err error) {
	n := len(data)
	pos := off
	totalLen := 0
	firstStart := -1
	contiguousEnd := -1

	for {
		if pos >= n {
			return 0, nil, 0, scratch, errIncompleteRequest
		}
		size, lineEnd, perr := parseChunkSizeLine(data, pos, n)
		if perr != nil {
			return 0, nil, 0, scratch, perr
		}
		pos = lineEnd

		if size == 0 {
			pos, perr = skipHTTP1ChunkTrailers(data, pos, n)
			if perr != nil {
				return 0, nil, 0, scratch, perr
			}
			if totalLen == 0 {
				return pos, nil, 0, scratch, nil
			}
			if firstStart >= 0 && contiguousEnd == firstStart+totalLen {
				return pos, data[firstStart:contiguousEnd], totalLen, scratch, nil
			}
			if cap(scratch) < totalLen {
				scratch = make([]byte, totalLen)
			} else {
				scratch = scratch[:totalLen]
			}
			copyScratch := scratch
			rpos := off
			acc := 0
			for {
				chunkSize, next, perr := parseChunkSizeLine(data, rpos, n)
				if perr != nil {
					return 0, nil, 0, scratch, perr
				}
				if chunkSize == 0 {
					break
				}
				chunkData := data[next : next+chunkSize]
				copy(copyScratch[acc:], chunkData)
				acc += chunkSize
				rpos = next + chunkSize + 2
			}
			return pos, scratch, totalLen, scratch, nil
		}

		if int64(totalLen+size) > maxBody {
			return 0, nil, 0, scratch, errPayloadTooLarge
		}
		if pos+size+2 > n {
			return 0, nil, 0, scratch, errIncompleteRequest
		}
		if data[pos+size] != '\r' || data[pos+size+1] != '\n' {
			return 0, nil, 0, scratch, errInvalidRequest
		}

		if firstStart < 0 {
			firstStart = pos
			contiguousEnd = pos + size
		} else if pos != contiguousEnd {
			contiguousEnd = -1
		} else {
			contiguousEnd = pos + size
		}
		totalLen += size
		pos += size + 2
	}
}

func parseChunkSizeLine(data []byte, pos, n int) (size int, next int, err error) {
	if pos >= n {
		return 0, 0, errIncompleteRequest
	}
	hasDigit := false
	i := pos
	for i < n {
		b := data[i]
		if b == '\r' {
			if i+1 >= n {
				return 0, 0, errIncompleteRequest
			}
			if data[i+1] != '\n' {
				return 0, 0, errInvalidRequest
			}
			if !hasDigit {
				return 0, 0, errInvalidRequest
			}
			return size, i + 2, nil
		}
		if b == ';' {
			for i < n && data[i] != '\r' {
				i++
			}
			continue
		}
		if b >= '0' && b <= '9' {
			hasDigit = true
			size = size*16 + int(b-'0')
			if size < 0 {
				return 0, 0, errInvalidRequest
			}
			i++
			continue
		}
		if b >= 'a' && b <= 'f' {
			hasDigit = true
			size = size*16 + int(b-'a'+10)
			i++
			continue
		}
		if b >= 'A' && b <= 'F' {
			hasDigit = true
			size = size*16 + int(b-'A'+10)
			i++
			continue
		}
		if b == ' ' || b == '\t' {
			i++
			continue
		}
		return 0, 0, errInvalidRequest
	}
	return 0, 0, errIncompleteRequest
}

func skipHTTP1ChunkTrailers(data []byte, pos, n int) (int, error) {
	for {
		if pos >= n {
			return 0, errIncompleteRequest
		}
		if data[pos] == '\r' {
			if pos+1 >= n {
				return 0, errIncompleteRequest
			}
			if data[pos+1] != '\n' {
				return 0, errInvalidRequest
			}
			return pos + 2, nil
		}
		for pos < n && data[pos] != '\r' {
			if data[pos] == 0 {
				return 0, errInvalidRequest
			}
			pos++
		}
		if pos+1 >= n {
			return 0, errIncompleteRequest
		}
		if data[pos+1] != '\n' {
			return 0, errInvalidRequest
		}
		pos += 2
	}
}
