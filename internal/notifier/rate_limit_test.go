package notifier

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenBucket_allowBurstThenRefill(t *testing.T) {
	t.Parallel()
	bucket := newTokenBucket(20)
	now := time.Now()
	for i := 0; i < 20; i++ {
		require.True(t, bucket.allow(now), "burst %d", i)
	}
	require.False(t, bucket.allow(now))
}

func TestTokenBucket_backoffBlocks(t *testing.T) {
	t.Parallel()
	bucket := newTokenBucket(20)
	now := time.Now()
	bucket.backoff(time.Minute)
	require.False(t, bucket.allow(now))
}

func TestProviderRateLimiter_perRecipient(t *testing.T) {
	t.Parallel()
	limiter := newProviderRateLimiter(map[string]int{"TELEGRAM": 2})
	require.True(t, limiter.Allow("TELEGRAM", "chat-a"))
	require.True(t, limiter.Allow("TELEGRAM", "chat-a"))
	require.False(t, limiter.Allow("TELEGRAM", "chat-a"))
	require.True(t, limiter.Allow("TELEGRAM", "chat-b"))
}

func TestProviderRateLimiter_backoff(t *testing.T) {
	limiter := newProviderRateLimiter(map[string]int{"TELEGRAM": 20})
	require.True(t, limiter.Allow("TELEGRAM", "chat-x"))
	limiter.Backoff("TELEGRAM", "chat-x", time.Minute)
	require.False(t, limiter.Allow("TELEGRAM", "chat-x"))
}

func TestParseTelegramRetryAfter_body(t *testing.T) {
	t.Parallel()
	body := []byte(`{"ok":false,"error_code":429,"parameters":{"retry_after":7}}`)
	got := parseTelegramRetryAfter(nil, body)
	require.Equal(t, 7*time.Second, got)
}

func TestParseTelegramRetryAfter_header(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: make(http.Header)}
	resp.Header.Set("Retry-After", "12")
	got := parseTelegramRetryAfter(resp, nil)
	require.Equal(t, 12*time.Second, got)
}
