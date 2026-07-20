//go:build linux

package installer

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

var (
	btfPathOverride               = ""
	cgroupControllersPathOverride = ""
	ethToolOutputOverride         = ""
)

func btfSysPath() string {
	if btfPathOverride != "" {
		return btfPathOverride
	}
	return "/sys/kernel/btf/vmlinux"
}

func cgroupControllersPath() string {
	if cgroupControllersPathOverride != "" {
		return cgroupControllersPathOverride
	}
	return "/sys/fs/cgroup/cgroup.controllers"
}

func getPreflightChecks() []checkFunc {
	return []checkFunc{
		checkKernelVersion,
		checkBTF,
		checkUlimit,
		checkPorts,
		checkNIC,
		checkLibs,
		checkSysctl,
	}
}

func checkKernelVersion() PreflightCheck {
	res := PreflightCheck{
		ID:          "PF-KERNEL",
		Description: "Kernel version ≥ 6.1",
		Status:      StatusPass,
	}

	if runtime.GOOS != "linux" {
		res.Status = StatusFail
		res.Message = "Not running on Linux"
		return res
	}

	out, err := exec.Command("uname", "-r").Output()
	if err != nil {
		res.Status = StatusFail
		res.Message = "Failed to get kernel version"
		return res
	}

	version := strings.Split(string(out), "-")[0]
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		res.Status = StatusFail
		res.Message = fmt.Sprintf("Malformed kernel version: %s", version)
		return res
	}

	major, _ := strconv.Atoi(parts[0])
	minor, _ := strconv.Atoi(parts[1])

	if major < 6 || (major == 6 && minor < 1) {
		res.Status = StatusFail
		res.Message = fmt.Sprintf("Current version: %s", version)
	}

	return res
}

func checkBTF() PreflightCheck {
	res := PreflightCheck{
		ID:          "PF-BTF",
		Description: "BTF support",
		Status:      StatusPass,
	}

	if _, err := os.Stat(btfSysPath()); os.IsNotExist(err) {
		res.Status = StatusWarn
		res.Message = "BTF support not found (/sys/kernel/btf/vmlinux missing). Required for edge_xdp."
	}

	return res
}

func checkUlimit() PreflightCheck {
	res := PreflightCheck{
		ID:          "PF-ULIMIT",
		Description: "Check open files limit",
		Status:      StatusPass,
	}

	f, err := os.Open("/proc/self/limits")
	if err != nil {
		res.Status = StatusWarn
		res.Message = "Could not check ulimit"
		return res
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "Max open files") {
			fields := strings.Fields(scanner.Text())
			if len(fields) >= 4 {
				limit, _ := strconv.Atoi(fields[3])
				if limit < 65535 {
					res.Status = StatusWarn
					res.Message = fmt.Sprintf("Limit too low: %d (recommend 65535)", limit)
				}
				return res
			}
		}
	}
	if err := scanner.Err(); err != nil {
		res.Status = StatusWarn
		res.Message = "Error reading /proc/self/limits"
	}

	return res
}

func checkPorts() PreflightCheck {
	res := PreflightCheck{
		ID:          "PF-PORTS",
		Description: "Check common ports (8080, 9090, 6379, 5432)",
		Status:      StatusPass,
	}

	ports := []int{8080, 9090, 6379, 5432}
	for _, port := range ports {
		if portInUse(port) {
			res.Status = StatusWarn
			res.Message = fmt.Sprintf("port %d appears in use", port)
			return res
		}
	}

	return res
}

func portInUse(port int) bool {
	addr := fmt.Sprintf(":%d", port)
	ln, err := exec.Command("ss", "-ltn", "sport", "=", fmt.Sprintf(":%d", port)).Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(ln), addr)
}

func checkNIC() PreflightCheck {
	res := PreflightCheck{
		ID:          "PF-NIC",
		Description: "Check for high-speed NIC",
		Status:      StatusPass,
	}

	output := ethToolOutputOverride
	if output == "" {
		out, err := exec.Command("ethtool", "-i", "eth0").CombinedOutput()
		if err != nil {
			res.Status = StatusWarn
			res.Message = "ethtool unavailable or eth0 missing"
			return res
		}
		output = string(out)
	}

	if !strings.Contains(strings.ToLower(output), "driver:") {
		res.Status = StatusWarn
		res.Message = "unable to read NIC driver info"
	}

	return res
}

func checkLibs() PreflightCheck {
	res := PreflightCheck{
		ID:          "PF-LIBS",
		Description: "Check for required libraries (libpcap, libelf)",
		Status:      StatusPass,
	}

	required := []string{"libpcap", "libelf"}
	for _, lib := range required {
		out, err := exec.Command("ldconfig", "-p").Output()
		if err != nil {
			res.Status = StatusWarn
			res.Message = "ldconfig unavailable"
			return res
		}
		if !strings.Contains(string(out), lib) {
			res.Status = StatusWarn
			res.Message = fmt.Sprintf("missing library: %s", lib)
			return res
		}
	}

	return res
}

func checkSysctl() PreflightCheck {
	res := PreflightCheck{
		ID:          "PF-SYSCTL",
		Description: "Check sysctl parameters",
		Status:      StatusPass,
	}

	data, err := os.ReadFile("/proc/sys/net/core/somaxconn")
	if err != nil {
		res.Status = StatusWarn
		res.Message = "unable to read net.core.somaxconn"
		return res
	}

	val, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || val < 4096 {
		res.Status = StatusWarn
		res.Message = fmt.Sprintf("net.core.somaxconn=%d (recommend >= 4096)", val)
	}

	return res
}
