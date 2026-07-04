package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// Secret wraps sensitive strings so structured logs never emit credentials in plain text.
type Secret string

// LogValue redacts secret fields when config values are logged through slog.
func (s Secret) LogValue() slog.Value {
	return slog.StringValue("**********")
}

// trimCommaList splits comma-separated env values and drops empty tokens so trailing commas do not create phantom shards.
func trimCommaList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		switch strings.ToLower(value) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if value, ok := os.LookupEnv(key); ok {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return fallback
}

// getEnvMicro converts dollar env values to micro-units so billing code stays integer-only.
func getEnvMicro(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return int64(floatVal * 1_000_000)
		}
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
	}
	return fallback
}
