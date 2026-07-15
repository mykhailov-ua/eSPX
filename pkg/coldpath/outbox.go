package coldpath

import (
	"encoding/json"
	"log/slog"
)

// UnmarshalStrict unmarshals an outbox payload and wraps decode failures.
func UnmarshalStrict[T any](payload []byte) (T, error) {
	var p T
	if err := json.Unmarshal(payload, &p); err != nil {
		slog.Error("invalid outbox payload", "error", err)
		return p, err
	}
	return p, nil
}

// UnmarshalLenient unmarshals an outbox payload; decode failures yield the zero value.
func UnmarshalLenient[T any](payload []byte) T {
	var p T
	if err := json.Unmarshal(payload, &p); err != nil {
		slog.Warn("invalid outbox payload", "error", err)
	}
	return p
}
