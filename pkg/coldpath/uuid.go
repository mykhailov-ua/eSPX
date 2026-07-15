package coldpath

import (
	"fmt"

	"github.com/google/uuid"
)

// ParseUUID parses s as a UUID for cold-path handlers and workers.
func ParseUUID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse uuid: %w", err)
	}
	return id, nil
}
