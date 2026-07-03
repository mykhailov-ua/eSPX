package filter

import (
	"context"
	"testing"

	"espx/internal/ads/catalog"
	"espx/internal/config"
	"espx/internal/domain"

	adstest "espx/internal/ads/testutil"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards DeviceFilter flags blocked TLS hashes and Client Hints mismatches.
func TestDeviceFilter_signals(t *testing.T) {
	cfg := &config.Config{}
	sw := catalog.NewSettingsWatcher([]redis.UniversalClient{redis.NewClient(&redis.Options{Addr: "127.0.0.1:9"})}, cfg)
	sw.SetSnapshotForTest(&catalog.DynamicConfig{
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

// Guards DeviceFilter is a no-op when TLS hash is absent and hints match UA.
func TestDeviceFilter_pass_clean_client(t *testing.T) {
	sw := catalog.NewSettingsWatcher(nil, &config.Config{})
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

// Guards FilterEngine wires DeviceFilter before unified Lua on the hot path.
func TestFilterEngine_deviceFilter_before_lua(t *testing.T) {
	sw := catalog.NewSettingsWatcher(nil, &config.Config{})
	sw.SetSnapshotForTest(&catalog.DynamicConfig{TLSHashBlocklist: "badja3"})
	deviceFilter := NewDeviceFilter(sw)

	registry := &adstest.MockRegistry{}
	engine := NewFilterEngine(0, deviceFilter)
	engine.SetRegistry(registry)

	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.Reset()
	evt.TLSHash = "badja3"

	before := testutil.ToFloat64(FilterEngineFailures)
	err := engine.Check(context.Background(), evt)
	require.Error(t, err)
	assert.Greater(t, testutil.ToFloat64(FilterEngineFailures), before)
}
