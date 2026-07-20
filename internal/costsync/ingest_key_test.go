package costsync

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func ingestKeyLegacy(customerID, campaignID uuid.UUID, date time.Time, network, placementID string, lineType LineType) string {
	return customerID.String() + "|" + campaignID.String() + "|" +
		date.Format("2006-01-02") + "|" + network + "|" + placementID + "|" + string(lineType)
}

func TestIngestKey_MatchesLegacyFormat(t *testing.T) {
	customerID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	campaignID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	date := time.Date(2026, 7, 1, 15, 30, 0, 0, time.FixedZone("MSK", 3*3600))

	got := IngestKey(customerID, campaignID, date, "facebook", "ad-1", LineTypeSpend)
	want := ingestKeyLegacy(customerID, campaignID, date, "facebook", "ad-1", LineTypeSpend)
	require.Equal(t, want, got)
	require.Contains(t, got, "2026-07-01")
}
