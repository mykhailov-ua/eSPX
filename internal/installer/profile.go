package installer

import (
	"errors"
	"fmt"
	"os"
)

// Profile names a supported deployment topology for espx-install.
type Profile string

const (
	ProfileSingleVPS  Profile = "single_vps"
	ProfileComposeDev Profile = "compose_dev"
	ProfileK8sK3s     Profile = "k8s_k3s"
)

// InstallProfile is the persisted install.yaml contract: topology, feature flags, and NIC binding.
type InstallProfile struct {
	Type             Profile `yaml:"profile"`
	EdgeXDP          bool    `yaml:"edge_xdp"`
	MultiRegion      bool    `yaml:"multi_region"`
	TelemetryEnabled bool    `yaml:"telemetry_enabled"`
	Interface        string  `yaml:"interface"`
}

// Validate enforces profile-specific constraints before configure/apply.
func (p *InstallProfile) Validate() error {
	switch p.Type {
	case ProfileSingleVPS, ProfileComposeDev, ProfileK8sK3s:
	default:
		return fmt.Errorf("invalid profile: %s", p.Type)
	}

	if p.EdgeXDP && p.Type == ProfileComposeDev {
		return errors.New("edge_xdp is not supported in compose_dev profile")
	}

	if p.EdgeXDP && !btfAvailable() {
		return errors.New("edge_xdp requires BTF support (PF-BTF)")
	}

	if p.MultiRegion && p.Type == ProfileComposeDev {
		return errors.New("multi_region is not supported in compose_dev profile")
	}

	if p.Interface == "" && (p.Type == ProfileSingleVPS || p.Type == ProfileK8sK3s) {
		return errors.New("network interface must be specified for production-like profiles")
	}

	if p.Type == ProfileK8sK3s && !cgroupV2Enabled() {
		return errors.New("k8s_k3s profile requires cgroup v2")
	}

	return nil
}

func btfAvailable() bool {
	if _, err := os.Stat(btfSysPath()); err == nil {
		return true
	}
	return false
}

func cgroupV2Enabled() bool {
	if _, err := os.Stat(cgroupControllersPath()); err == nil {
		return true
	}
	return false
}
