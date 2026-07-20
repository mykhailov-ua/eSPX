//go:build !linux

package installer

func getPreflightChecks() []checkFunc {
	return []checkFunc{
		func() PreflightCheck {
			return PreflightCheck{
				ID:          "PF-PLATFORM",
				Description: "Linux host required",
				Status:      StatusFail,
				Message:     "preflight is supported on Linux only",
			}
		},
	}
}

func btfSysPath() string            { return "/sys/kernel/btf/vmlinux" }
func cgroupControllersPath() string { return "/sys/fs/cgroup/cgroup.controllers" }
