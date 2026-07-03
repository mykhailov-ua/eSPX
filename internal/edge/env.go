// Package edge holds cold-path helpers for ingress hardening binaries and BPF sync.
package edge

import (
	"os"
	"strings"
	"time"
)

// EnvOr returns the environment variable value or fallback when unset.
func EnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvDuration parses a duration environment variable or returns fallback on missing/invalid input.
func EnvDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return d
}

// FirstRedisAddr returns shard-0 host:port from REDIS_ADDRS or REDIS_HOST/REDIS_PORT.
func FirstRedisAddr() string {
	if addrs := os.Getenv("REDIS_ADDRS"); addrs != "" {
		first := strings.TrimSpace(strings.Split(addrs, ",")[0])
		if first != "" {
			return first
		}
	}
	host := EnvOr("REDIS_HOST", "127.0.0.1")
	port := EnvOr("REDIS_PORT", "6379")
	return host + ":" + port
}
