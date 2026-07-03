//go:build !linux

package server

import (
	"os/exec"
	"testing"
	"time"
)

func chaosLabOnLinux() bool {
	return false
}

func stressNgAvailable() bool {
	return false
}

func cpulimitAvailable() bool {
	return false
}

func startStressNgHDD(t *testing.T, scratchFile string, sizeMB int, timeout time.Duration) (*exec.Cmd, error) {
	t.Helper()
	return nil, exec.ErrNotFound
}

func throttleProcessCPU(t *testing.T, pid int, limitPct int) (*exec.Cmd, error) {
	t.Helper()
	return nil, exec.ErrNotFound
}
