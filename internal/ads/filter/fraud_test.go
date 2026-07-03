package filter

import (
	"context"
	"github.com/google/uuid"
	"testing"
	"time"

	"espx/internal/ads/sharding"
	"espx/internal/domain"

	adstest "espx/internal/ads/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards datacenter IPs are L2-shadowed with a single high-confidence signal.
func TestFraudFilter_DatacenterIP_ReturnsFraudDetected(t *testing.T) {
	geo := &MockGeoProvider{}
	f := NewFraudFilter(geo)
	registry := &adstest.MockRegistry{}

	evt := &domain.Event{
		Type:         "click",
		UserID:       "user1",
		CampaignID:   uuid.New(),
		IP:           "1.1.1.66",
		StringBuffer: make([]byte, 0, 64),
	}

	engine := NewFilterEngine(0, f)
	engine.SetRegistry(registry)
	err := engine.Check(context.Background(), evt)
	require.NoError(t, err)
	assert.True(t, evt.ShadowEvent)
	assert.Equal(t, FraudReasonCodeDatacenterIP, evt.FraudReason)
	assert.Equal(t, uint32(FraudSignalWeight(FraudReasonDatacenterIP)), evt.FraudScore)
}

func TestFraudFilter_DualL1_ReturnsFraudDetected(t *testing.T) {
	geo := &MockGeoProvider{}
	fraud := NewFraudFilter(geo)
	uf := NewUnifiedFilter(
		[]redis.UniversalClient{&lowTTCRedis{}},
		sharding.NewJumpHashSharder(1),
		&adstest.MockRegistry{},
		nil,
		0,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10_000,
	)

	evt := &domain.Event{
		Type:         "click",
		UserID:       "user1",
		CampaignID:   uuid.New(),
		IP:           "1.1.1.66",
		ClickID:      "c1",
		StringBuffer: make([]byte, 0, 64),
	}

	engine := NewFilterEngine(0, fraud, uf)
	engine.SetRegistry(&adstest.MockRegistry{})
	err := engine.Check(context.Background(), evt)
	require.ErrorIs(t, err, ErrFraudDetected)
	assert.False(t, evt.ShadowEvent)
	assert.Contains(t, evt.FraudReason, FraudReasonCodeDatacenterIP)
	assert.Contains(t, evt.FraudReason, FraudReasonCodeLowTTC)
}
