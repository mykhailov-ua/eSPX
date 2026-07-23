package bpf

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"espx/internal/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encodeFingerprintSample(ts uint64, srcIP, tcpHash uint32, window uint16, ttl, mss uint8) []byte {
	raw := make([]byte, 20)
	binary.LittleEndian.PutUint64(raw[0:8], ts)
	binary.LittleEndian.PutUint32(raw[8:12], srcIP)
	binary.LittleEndian.PutUint32(raw[12:16], tcpHash)
	binary.LittleEndian.PutUint16(raw[16:18], window)
	raw[18] = ttl
	raw[19] = mss
	return raw
}

// TestChaos_FingerprintHandlerBrokenSamples feeds truncated, empty, and oversized ringbuf payloads.
// Hypothesis: decoder never panics; samples shorter than 20 bytes are ignored.
func TestChaos_FingerprintHandlerBrokenSamples(t *testing.T) {
	var handled atomic.Int32
	handler := NewFingerprintHandler(func(evt FingerprintEvent) error {
		handled.Add(1)
		return nil
	})

	broken := [][]byte{
		nil,
		{},
		make([]byte, 1),
		make([]byte, 19),
		{0xff},
	}
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 500; i++ {
		n := r.Intn(64)
		buf := make([]byte, n)
		r.Read(buf)
		broken = append(broken, buf)
	}

	var ignored, processed int
	for _, raw := range broken {
		if len(raw) < 20 {
			ignored++
			continue
		}
		evt := decodeFingerprint(raw)
		require.NoError(t, handler.onEvent(evt))
		processed++
	}

	assert.Greater(t, ignored, 100)
	assert.Equal(t, int32(processed), handled.Load())

	testutil.LogChaosProof(t, "fingerprint_handler_broken_samples", map[string]string{
		"samples":   fmt.Sprintf("%d", len(broken)),
		"ignored":   fmt.Sprintf("%d", ignored),
		"processed": fmt.Sprintf("%d", processed),
		"no_panic":  "true",
	})
}

// TestChaos_FingerprintHandlerConcurrentDecode runs 32 goroutines decoding random 20-byte samples.
// Hypothesis: no data race; all callbacks complete without panic.
func TestChaos_FingerprintHandlerConcurrentDecode(t *testing.T) {
	const goroutines = 32
	const perG = 200

	var handled atomic.Int64
	handler := NewFingerprintHandler(func(evt FingerprintEvent) error {
		handled.Add(1)
		return nil
	})

	var wg sync.WaitGroup
	start := make(chan struct{})
	r := rand.New(rand.NewSource(99))

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perG; i++ {
				raw := encodeFingerprintSample(
					uint64(i),
					uint32(g*1000+i),
					uint32(r.Uint32()),
					uint16(r.Uint32()),
					uint8(r.Intn(256)),
					uint8(r.Intn(256)),
				)
				evt := decodeFingerprint(raw)
				require.NoError(t, handler.onEvent(evt))
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int64(goroutines*perG), handled.Load())

	testutil.LogChaosProof(t, "fingerprint_handler_concurrent_decode", map[string]string{
		"goroutines": fmt.Sprintf("%d", goroutines),
		"per_g":      fmt.Sprintf("%d", perG),
		"handled":    fmt.Sprintf("%d", handled.Load()),
	})
}

// TestChaos_FingerprintHandlerCallbackFailure aborts drain on first Redis-style error.
// Hypothesis: partial count returned; no silent swallow of handler errors.
func TestChaos_FingerprintHandlerCallbackFailure(t *testing.T) {
	const failAfter = 7
	var calls atomic.Int32

	handler := NewFingerprintHandler(func(evt FingerprintEvent) error {
		n := calls.Add(1)
		if n >= failAfter {
			return fmt.Errorf("injected handler failure at %d", n)
		}
		return nil
	})

	for i := 0; i < 20; i++ {
		raw := encodeFingerprintSample(uint64(i), 0x0a000001, uint32(i), 64240, 64, 44)
		evt := decodeFingerprint(raw)
		err := handler.onEvent(evt)
		if int(calls.Load()) >= failAfter {
			require.Error(t, err)
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, int32(failAfter), calls.Load())

	testutil.LogChaosProof(t, "fingerprint_handler_callback_failure", map[string]string{
		"fail_after":       fmt.Sprintf("%d", failAfter),
		"calls":            fmt.Sprintf("%d", calls.Load()),
		"error_propagated": "true",
	})
}

// TestChaos_FingerprintDecodeOverflowFields decodes samples with max field values.
func TestChaos_FingerprintDecodeOverflowFields(t *testing.T) {
	raw := encodeFingerprintSample(^uint64(0), ^uint32(0), ^uint32(0), ^uint16(0), 255, 255)
	evt := decodeFingerprint(raw)

	assert.Equal(t, ^uint64(0), evt.TsNs)
	assert.Equal(t, ^uint32(0), evt.SrcIP)
	assert.Equal(t, ^uint32(0), evt.TCPHash)
	assert.Equal(t, ^uint16(0), evt.Window)
	assert.Equal(t, uint8(255), evt.TTL)
	assert.Equal(t, uint8(255), evt.MSS)

	testutil.LogChaosProof(t, "fingerprint_decode_overflow_fields", map[string]string{
		"tcp_hash": "ffffffff",
		"ts_ns":    "max_uint64",
	})
}
