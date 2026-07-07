package management

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseAPIPagination_defaultsAndCaps(t *testing.T) {
	cases := []struct {
		query      string
		wantLimit  int32
		wantOffset int32
	}{
		{"", 50, 0},
		{"limit=10", 10, 0},
		{"limit=1000", 1000, 0},
		{"limit=5000", 1000, 0},
		{"offset=25", 50, 25},
		{"limit=10&offset=5", 10, 5},
		{"limit=0", 50, 0},
		{"limit=-1", 50, 0},
		{"offset=0", 50, 0},
	}

	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "/api/v1/recon/runs?"+tc.query, nil)
			limit, offset := parseAPIPagination(req)
			assert.Equal(t, tc.wantLimit, limit, "limit")
			assert.Equal(t, tc.wantOffset, offset, "offset")
		})
	}
}
