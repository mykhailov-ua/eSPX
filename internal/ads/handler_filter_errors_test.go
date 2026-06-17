package ads

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Redis stub with injectable XAdd for fraud stream error tests.
type mockRedisXAdd struct {
	mockRedisClient
}

func (m *mockRedisXAdd) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	cmd.SetVal("1-0")
	return cmd
}

// Guards gnet handler maps filter errors to correct HTTP status and metrics.
func TestAdsPacketHandler_FilterErrors(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
		FilterTimeoutMs:    50,
		StreamMaxLen:       1000,
	}
	sharder := NewJumpHashSharder(1)
	registry := &mockRegistry{}

	makeReq := func() parsedHTTPRequest {
		payload := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
		return parsedHTTPRequest{
			Method:           []byte("POST"),
			Path:             []byte("/track"),
			ContentType:      []byte("application/json"),
			Body:             payload,
			ContentLength:    len(payload),
			HasContentLength: true,
		}
	}

	t.Run("ErrCampaignNotFound -> 404", func(t *testing.T) {
		h := NewAdsPacketHandler(cfg, registry, NewFilterEngine(0, &errFilter{err: ErrCampaignNotFound}), nil, nil, sharder, "fraud", nil)
		conn := &mockGnetConn{written: make([]byte, 0, 512)}
		h.React(makeReq(), conn)
		assert.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 404")))
	})

	t.Run("ErrBidFloorNotMet -> 402", func(t *testing.T) {
		h := NewAdsPacketHandler(cfg, registry, NewFilterEngine(0, &errFilter{err: ErrBidFloorNotMet}), nil, nil, sharder, "fraud", nil)
		conn := &mockGnetConn{written: make([]byte, 0, 512)}
		h.React(makeReq(), conn)
		assert.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 402")))
	})

	t.Run("filter timeout -> 504", func(t *testing.T) {
		h := NewAdsPacketHandler(cfg, registry, NewFilterEngine(50*time.Millisecond, &slowFilter{delay: 200 * time.Millisecond}), nil, nil, sharder, "fraud", nil)
		conn := &mockGnetConn{written: make([]byte, 0, 512)}
		h.React(makeReq(), conn)
		assert.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 504")))
	})

	t.Run("ErrFraudDetected -> 202 silent accept", func(t *testing.T) {
		rdb := &mockRedisXAdd{}
		h := NewAdsPacketHandler(cfg, registry, NewFilterEngine(0, &errFilter{err: ErrFraudDetected}), nil, []redis.UniversalClient{rdb}, sharder, "fraud-stream", nil)
		conn := &mockGnetConn{written: make([]byte, 0, 512)}
		h.React(makeReq(), conn)
		assert.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 202")))
	})

	t.Run("redis circuit open -> 503 Retry-After", func(t *testing.T) {
		h := NewAdsPacketHandler(cfg, registry, NewFilterEngine(0, &errFilter{err: database.ErrRedisCircuitOpen}), nil, nil, sharder, "fraud", nil)
		conn := &mockGnetConn{written: make([]byte, 0, 512)}
		h.React(makeReq(), conn)
		assert.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 503")))
		assert.True(t, bytes.Contains(conn.written, []byte("Retry-After: 1")))
	})
}

// Guards HTTP track handler maps filter errors to correct status codes.
func TestTrackHandler_FilterErrors(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
		FilterTimeoutMs:    50,
	}
	sharder := NewJumpHashSharder(1)
	registry := &mockRegistry{}

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)

	t.Run("ErrCampaignNotFound -> 404", func(t *testing.T) {
		handler := NewRouter(cfg, registry, NewFilterEngine(0, &errFilter{err: ErrCampaignNotFound}), nil, nil, sharder, "fraud", nil)
		req := httptest.NewRequest(http.MethodPost, "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("ErrBidFloorNotMet -> 402", func(t *testing.T) {
		handler := NewRouter(cfg, registry, NewFilterEngine(0, &errFilter{err: ErrBidFloorNotMet}), nil, nil, sharder, "fraud", nil)
		req := httptest.NewRequest(http.MethodPost, "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusPaymentRequired, w.Code)
	})

	t.Run("filter timeout -> 504", func(t *testing.T) {
		handler := NewRouter(cfg, registry, NewFilterEngine(50*time.Millisecond, &slowFilter{delay: 200 * time.Millisecond}), nil, nil, sharder, "fraud", nil)
		req := httptest.NewRequest(http.MethodPost, "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusGatewayTimeout, w.Code)
	})

	t.Run("ErrFraudDetected -> 202 silent accept", func(t *testing.T) {
		rdb := &mockRedisXAdd{}
		handler := NewRouter(cfg, registry, NewFilterEngine(0, &errFilter{err: ErrFraudDetected}), nil, []redis.UniversalClient{rdb}, sharder, "fraud-stream", nil)
		req := httptest.NewRequest(http.MethodPost, "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusAccepted, w.Code)
	})

	t.Run("redis circuit open -> 503 Retry-After", func(t *testing.T) {
		handler := NewRouter(cfg, registry, NewFilterEngine(0, &errFilter{err: database.ErrRedisCircuitOpen}), nil, nil, sharder, "fraud", nil)
		req := httptest.NewRequest(http.MethodPost, "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Equal(t, "1", w.Header().Get("Retry-After"))
	})
}
