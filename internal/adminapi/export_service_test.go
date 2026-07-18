package adminapi

import (
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestJobRunner_CreateJob_invalidCustomer(t *testing.T) {
	t.Parallel()
	runner := NewJobRunner(&CompositeReadService{}, t.TempDir())
	_, err := runner.CreateJob(t.Context(), JobSpec{
		CustomerID: "not-a-uuid",
		From:       time.Now().UTC().Format(time.RFC3339),
		To:         time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		Format:     "csv",
	})
	require.Error(t, err)
}

func TestJobRunner_GetJob_missing(t *testing.T) {
	t.Parallel()
	runner := NewJobRunner(&CompositeReadService{}, t.TempDir())
	_, ok := runner.GetJob("missing")
	assert.False(t, ok)
}

func TestJobSpec_formatValidation(t *testing.T) {
	t.Parallel()
	runner := NewJobRunner(&CompositeReadService{}, t.TempDir())
	_, err := runner.CreateJob(t.Context(), JobSpec{
		CustomerID: "550e8400-e29b-41d4-a716-446655440000",
		From:       "2026-06-01T00:00:00Z",
		To:         "2026-06-30T00:00:00Z",
		Format:     "xml",
	})
	require.Error(t, err)
}
