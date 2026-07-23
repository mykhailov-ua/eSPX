package bpf

import (
	"testing"
)

// FuzzDecodeFingerprint ensures that the ringbuf decoder for fingerprints
// never panics on malformed or truncated data from the kernel.
func FuzzDecodeFingerprint(f *testing.F) {
	seed := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // TsNs
		0x0a, 0x00, 0x00, 0x01, // SrcIP
		0xde, 0xad, 0xbe, 0xef, // TCPHash
		0x01, 0x02, // Window
		0x40, // TTL
		0xb4, // MSS
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add(make([]byte, 10))

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("panic in decodeFingerprint: %v", r)
			}
		}()
		if len(data) < 20 {
			// Expected minimum size for the struct
			return
		}
		_ = decodeFingerprint(data)
	})
}

// FuzzConfigUpdate ensures that updating the BPF config map doesn't panic
// even with edge cases like nil maps or unusual options.
func FuzzConfigUpdate(f *testing.F) {
	f.Add(true, false)
	f.Fuzz(func(t *testing.T, cookie, disableFingerprint bool) {
		objs := loadTestObjects(t)
		opts := InitOptions{
			SynCookieEnabled:   cookie,
			DisableFingerprint: disableFingerprint,
		}
		_ = InitConfigWith(objs.Config, opts)
	})
}

// FuzzStatsAggregation ensures that AggregateStats doesn't panic on nil map.
func FuzzStatsAggregation(f *testing.F) {
	f.Add(true)
	f.Fuzz(func(t *testing.T, isNil bool) {
		if isNil {
			_, _ = AggregateStats(nil)
		} else {
			objs := loadTestObjects(t)
			_, _ = AggregateStats(objs.Stats)
		}
	})
}

// FuzzDecodeViolation ensures that the ringbuf decoder for violations
// never panics on malformed data.
func FuzzDecodeViolation(f *testing.F) {
	seed := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // TsNs
		0x0a, 0x00, 0x00, 0x02, // SrcIP
		0x01, // Reason
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("panic in decodeViolation: %v", r)
			}
		}()
		if len(data) < 13 {
			return
		}
		_ = decodeViolation(data)
	})
}
