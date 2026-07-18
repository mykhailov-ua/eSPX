package adminapi

import (
	"net/http"
	"strconv"
	"time"

	"espx/pkg/coldpath"
)

const maxStatsRange = 90 * 24 * time.Hour

type invalidQueryError string

func errInvalidQuery(msg string) error {
	return invalidQueryError(msg)
}

func (e invalidQueryError) Error() string { return string(e) }

func parseStatsQuery(r *http.Request) (from, to time.Time, granularity string, err error) {
	granularity = r.URL.Query().Get("granularity")
	if granularity == "" {
		granularity = "hour"
	}
	if granularity != "hour" {
		return time.Time{}, time.Time{}, "", errInvalidQuery("granularity must be hour")
	}

	now := time.Now().UTC().Truncate(time.Hour)
	to = now
	from = now.Add(-7 * 24 * time.Hour)

	if toStr := r.URL.Query().Get("to"); toStr != "" {
		to, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			return time.Time{}, time.Time{}, "", errInvalidQuery("invalid to timestamp")
		}
		to = to.UTC()
	}
	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		from, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return time.Time{}, time.Time{}, "", errInvalidQuery("invalid from timestamp")
		}
		from = from.UTC()
	}

	if !to.After(from) {
		return time.Time{}, time.Time{}, "", errInvalidQuery("to must be after from")
	}
	if to.Sub(from) > maxStatsRange {
		return time.Time{}, time.Time{}, "", errInvalidQuery("time range exceeds 90 days")
	}
	return from, to, granularity, nil
}

func parseAPIPagination(r *http.Request) (int32, int32) {
	limit := int32(50)
	if l, err := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32); err == nil && l > 0 {
		limit = int32(l)
	}
	offset := int32(0)
	if o, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 32); err == nil && o > 0 {
		offset = int32(o)
	}
	return coldpath.ClampLimitOffset(limit, offset, 50, 1000)
}
