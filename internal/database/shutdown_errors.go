package database

import (
	"context"
	"errors"
	"strings"
)

// IsPoolClosedError reports Postgres pool teardown errors during worker shutdown.
func IsPoolClosedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "closed pool")
}

// IsShutdownError reports errors expected while pools or clients are closing.
func IsShutdownError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if IsPoolClosedError(err) {
		return true
	}
	return strings.Contains(err.Error(), "client is closed")
}
