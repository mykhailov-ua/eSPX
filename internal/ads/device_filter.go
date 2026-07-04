package ads

import (
	"context"
	"hash/crc32"
	"strings"
	"sync"

	"espx/internal/domain"
)

// DeviceFilter flags TLS fingerprint and Client Hints mismatches on the ingest path.
type DeviceFilter struct {
	settings   *SettingsWatcher
	blockedTLS map[uint32]struct{}
	mu         sync.RWMutex
}

func NewDeviceFilter(settings *SettingsWatcher) *DeviceFilter {
	f := &DeviceFilter{settings: settings}
	f.reloadBlocklist()
	return f
}

func (f *DeviceFilter) reloadBlocklist() {
	if f.settings == nil {
		return
	}
	cfg := f.settings.Get()
	m := make(map[uint32]struct{})
	for _, h := range parseCommaList(cfg.TLSHashBlocklist) {
		m[crc32.ChecksumIEEE([]byte(h))] = struct{}{}
	}
	f.mu.Lock()
	f.blockedTLS = m
	f.mu.Unlock()
}

// Check records device integrity fraud signals without blocking on its own.
func (f *DeviceFilter) Check(ctx context.Context, evt *domain.Event) error {
	if evt == nil {
		return nil
	}
	f.mu.RLock()
	blocked := f.blockedTLS
	f.mu.RUnlock()

	if evt.TLSHash != "" {
		h := crc32.ChecksumIEEE([]byte(evt.TLSHash))
		if _, onList := blocked[h]; onList {
			addFraudSignal(evt, FraudReasonTLSBlocklist)
		}
	}
	if deviceHintsMismatch(evt.SecCHUA, evt.UA) {
		addFraudSignal(evt, FraudReasonDeviceMismatch)
	}
	return nil
}

func parseCommaList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func deviceHintsMismatch(secCHUA, ua string) bool {
	if secCHUA == "" {
		return false
	}
	if ua == "" {
		return true
	}
	if strings.Contains(secCHUA, "Chrome") &&
		!strings.Contains(ua, "Chrome") &&
		!strings.Contains(ua, "Chromium") {
		return true
	}
	return false
}
