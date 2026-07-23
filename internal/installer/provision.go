package installer

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"gopkg.in/yaml.v3"
)

type packageManifest struct {
	Debian []string `yaml:"debian"`
}

func RunProvision(yes bool) error {
	data, err := os.ReadFile(packagesYAMLPath())
	if err != nil {
		return fmt.Errorf("read packages.yaml: %w", err)
	}

	var manifest packageManifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("parse packages.yaml: %w", err)
	}

	packages := manifest.Debian
	if len(packages) == 0 {
		return fmt.Errorf("no debian packages listed in %s", packagesYAMLPath())
	}

	if !yes {
		fmt.Printf("The following packages will be installed: %v\n", packages)
		fmt.Print("Proceed? [y/N]: ")
		var response string
		_, _ = fmt.Scanln(&response)
		if strings.ToLower(response) != "y" {
			return fmt.Errorf("provisioning aborted by user")
		}
	}

	args := append([]string{"apt-get", "install", "-y", "--no-upgrade"}, packages...)
	cmd := exec.Command("sudo", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("apt-get install failed: %w", err)
	}

	fmt.Println("Provisioning complete.")
	return nil
}
