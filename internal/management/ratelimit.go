package management

import (
	"net"
	"net/http"
	"strings"
	"sync"

	"espx/pkg/httpresponse"
	"golang.org/x/time/rate"
)

// ipRateLimiter applies per-client token buckets so one noisy peer cannot starve the whole gateway.
type ipRateLimiter struct {
	mu      sync.Mutex
	limit   rate.Limit
	burst   int
	entries map[string]*rate.Limiter
}

func newIPRateLimiter(rps float64, burst int) *ipRateLimiter {
	if rps <= 0 {
		rps = 10
	}
	if burst <= 0 {
		burst = 50
	}
	return &ipRateLimiter{
		limit:   rate.Limit(rps),
		burst:   burst,
		entries: make(map[string]*rate.Limiter),
	}
}

func (l *ipRateLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	lim, ok := l.entries[ip]
	if !ok {
		lim = rate.NewLimiter(l.limit, l.burst)
		l.entries[ip] = lim
	}
	return lim.Allow()
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

// limitByIP wraps handlers with a per-client IP token bucket.
func (h *Handler) limitByIP(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.ipLimiter != nil && !h.ipLimiter.allow(clientIP(r)) {
			httpresponse.Error(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS", "too many requests")
			return
		}
		next(w, r)
	}
}
