package bpf

import (
	"os"

	"github.com/cilium/ebpf"
)

// Default edge filter tunables — must match DEFAULT_* in edge_filter.c.
const (
	DefaultSynLimit       = 64
	DefaultPPSRate        = 2000
	DefaultGlobalSynLimit = 50000
	DefaultAssumedCPUs    = 8
)

// InitOptions controls runtime config map seed values.
type InitOptions struct {
	SynCookieEnabled   bool
	DisableFingerprint bool // fingerprint defaults on; set to disable
}

// DefaultConfig returns production defaults for the config ARRAY map.
func DefaultConfig(opts InitOptions) EdgeEdgeConfig {
	cfg := EdgeEdgeConfig{
		SynLimit:           DefaultSynLimit,
		PpsRate:            DefaultPPSRate,
		GlobalSynLimit:     DefaultGlobalSynLimit,
		AssumedCpus:        DefaultAssumedCPUs,
		SynSubnetLimit:     DefaultSynSubnetLimit,
		SynCookieEnabled:   0,
		FingerprintEnabled: 1,
	}
	if opts.SynCookieEnabled {
		cfg.SynCookieEnabled = 1
	}
	if opts.DisableFingerprint {
		cfg.FingerprintEnabled = 0
	}
	return cfg
}

// InitConfigFromEnv writes defaults into the config map using process environment.
func InitConfigFromEnv(m *ebpf.Map) error {
	opts := InitOptions{}
	if v := os.Getenv("XDP_SYN_COOKIE"); v == "1" || v == "true" {
		opts.SynCookieEnabled = true
	}
	if v := os.Getenv("XDP_FINGERPRINT"); v == "0" || v == "false" {
		opts.DisableFingerprint = true
	}
	return InitConfigWith(m, opts)
}

// InitConfig writes default tunables into the config map (index 0).
func InitConfig(m *ebpf.Map) error {
	return InitConfigWith(m, InitOptions{})
}

// InitConfigWith writes explicit tunables into the config map (index 0).
func InitConfigWith(m *ebpf.Map, opts InitOptions) error {
	if m == nil {
		return nil
	}
	key := uint32(0)
	cfg := DefaultConfig(opts)
	return m.Update(&key, &cfg, ebpf.UpdateAny)
}

// SynCookieEnabled returns whether XDP_SYN_COOKIE is enabled.
func SynCookieEnabled() bool {
	v := os.Getenv("XDP_SYN_COOKIE")
	return v == "1" || v == "true"
}
