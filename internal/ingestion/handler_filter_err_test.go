package ingestion

import (
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// TestClassifyFilterErr_HandlerMatrix covers all filterRejectKind values via gnet handler (M6-06).
func TestClassifyFilterErr_HandlerMatrix(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
		FilterTimeoutMs:    50,
		StreamMaxLen:       1000,
	}
	registry := &mockRegistry{}
	sharder := NewStaticSlotSharder(4)

	for _, tc := range rejectMatrixCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			timeout := time.Duration(cfg.FilterTimeoutMs) * time.Millisecond
			if tc.name == "filter_timeout" {
				timeout = 50 * time.Millisecond
			}
			var filter EventFilter
			switch {
			case tc.name == "filter_timeout":
				filter = &slowFilter{delay: 200 * time.Millisecond}
			case tc.name == "redis_circuit":
				filter = &errFilter{err: database.ErrRedisCircuitOpen}
			default:
				filter = tc.filter
			}
			h := NewAdsPacketHandler(cfg, registry, NewFilterEngine(timeout, filter), nil, nil, sharder, "fraud-stream", nil)
			body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
			status, _ := PostTrackGnetJSON(h, body)
			assert.Equal(t, tc.wantStatus, status)
		})
	}
}
