package cold

// ClampLimitOffset normalizes pagination with defaults and caps.
func ClampLimitOffset(limit, offset, defaultLimit, maxLimit int32) (int32, int32) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
