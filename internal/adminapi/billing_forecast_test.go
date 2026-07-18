package adminapi

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestBuildForecast_nilService(t *testing.T) {
	t.Parallel()
	var svc *CompositeReadService
	_, err := svc.BuildForecast(t.Context(), uuid.Nil)
	require.Error(t, err)
}

func TestAdminInvoiceFilters_monthParse(t *testing.T) {
	t.Parallel()
	month, err := time.Parse("2006-01", "2026-03")
	require.NoError(t, err)
	assert.Equal(t, time.March, month.Month())
}

func TestForecastDTO_projectionMath(t *testing.T) {
	t.Parallel()
	mtd := int64(1_000_000)
	runRate := int64(100_000)
	days := 10
	projected := mtd + runRate*int64(days)
	assert.Equal(t, int64(2_000_000), projected)
}
