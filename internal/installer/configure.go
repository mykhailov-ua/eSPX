package installer

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

func RunConfigure(interactive bool) error {
	profile := &InstallProfile{
		Type:             ProfileComposeDev,
		TelemetryEnabled: true,
	}

	if interactive {
		fmt.Println("eSPX Configuration Wizard")
		fmt.Println("-------------------------")

		fmt.Print("Choose profile (single_vps, compose_dev, k8s_k3s) [compose_dev]: ")
		var pStr string
		fmt.Scanln(&pStr)
		if pStr != "" {
			profile.Type = Profile(pStr)
		}

		fmt.Print("Enable Edge XDP? (y/N): ")
		var xdp string
		fmt.Scanln(&xdp)
		profile.EdgeXDP = strings.ToLower(xdp) == "y"

		fmt.Print("Network interface [eth0]: ")
		var iface string
		fmt.Scanln(&iface)
		if iface == "" {
			profile.Interface = "eth0"
		} else {
			profile.Interface = iface
		}
	}

	if err := profile.Validate(); err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	data, err := yaml.Marshal(profile)
	if err != nil {
		return err
	}

	err = os.WriteFile("install.yaml", data, 0644)
	if err != nil {
		return err
	}

	fmt.Println("Configuration saved to install.yaml")
	return nil
}
