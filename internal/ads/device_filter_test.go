package ads

import (
	"context"
	"testing"

	"espx/internal/config"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeviceFilter_signals(t *testing.T) {
	cfg := &config.Config{}
	sw := NewSettingsWatcher([]redis.UniversalClient{redis.NewClient(&redis.Options{Addr: "127.0.0.1:9"})}, cfg)
	sw.snapshot.Store(&DynamicConfig{
		TLSHashBlocklist: "abc123def,deadbeef",
	})
	f := NewDeviceFilter(sw)

	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.Reset()
	acc := attachFraudAccumulator(evt)
	defer releaseFraudAccumulator(evt, acc)

	evt.TLSHash = "abc123def"
	evt.SecCHUA = `"Google Chrome";v="120"`
	evt.UA = "curl/8.0"

	require.NoError(t, f.Check(context.Background(), evt))
	assert.True(t, acc.has(FraudReasonTLSBlocklist))
	assert.True(t, acc.has(FraudReasonDeviceMismatch))
}

func TestDeviceFilter_pass_clean_client(t *testing.T) {
	sw := NewSettingsWatcher(nil, &config.Config{})
	f := NewDeviceFilter(sw)

	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.Reset()
	acc := attachFraudAccumulator(evt)
	defer releaseFraudAccumulator(evt, acc)

	evt.UA = "Mozilla/5.0 Chrome/120"
	evt.SecCHUA = `"Google Chrome";v="120"`

	require.NoError(t, f.Check(context.Background(), evt))
	assert.Equal(t, uint8(0), acc.count)
}

func TestFilterEngine_deviceFilter_before_lua(t *testing.T) {
	sw := NewSettingsWatcher(nil, &config.Config{})
	sw.snapshot.Store(&DynamicConfig{TLSHashBlocklist: "badja3"})
	deviceFilter := NewDeviceFilter(sw)

	registry := &mockRegistry{}
	engine := NewFilterEngine(0, deviceFilter)
	engine.SetRegistry(registry)

	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.Reset()
	evt.CampaignID = uuid.New()
	evt.TLSHash = "badja3"

	require.NoError(t, engine.Check(context.Background(), evt))
	assert.Greater(t, evt.FraudScore, uint32(0))
}
