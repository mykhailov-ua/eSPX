package ads

import (
	"context"
	"testing"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards datacenter IPs are rejected with fraud reason set.
func TestFraudFilter_DatacenterIP_ReturnsFraudDetected(t *testing.T) {
	geo := &MockGeoProvider{}
	f := NewFraudFilter(geo)

	evt := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: uuid.New(),
		IP:         "1.1.1.66",
	}

	err := f.Check(context.Background(), evt)
	require.ErrorIs(t, err, ErrFraudDetected)
	assert.Equal(t, "datacenter_ip", evt.FraudReason)
}
