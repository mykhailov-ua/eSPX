package installer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

func renderTemplates(profile *InstallProfile, dryRun bool) error {
	unit, err := renderSystemdUnit(profile)
	if err != nil {
		return err
	}

	secrets := []byte("REDIS_URL=redis://localhost:6379\nESPX_TELEMETRY_ENABLED=" + boolString(profile.TelemetryEnabled) + "\n")

	manifests := []struct {
		path    string
		content []byte
		mode    os.FileMode
	}{
		{systemdUnitPath("espx-tracker.service"), unit, 0644},
		{secretsPath(), secrets, 0600},
	}

	for _, m := range manifests {
		if dryRun {
			fmt.Printf("[Dry-Run] Would render %s (sha256=%s)\n", m.path, checksum(m.content))
			continue
		}

		if unchanged, err := fileUnchanged(m.path, m.content); err != nil {
			return err
		} else if unchanged {
			fmt.Printf("Skipping %s (unchanged)\n", m.path)
			continue
		}

		if err := writeFile(m.path, m.content, m.mode); err != nil {
			return err
		}
		fmt.Printf("Rendered %s\n", m.path)
	}

	switch profile.Type {
	case ProfileComposeDev:
		script := filepath.Join(repoRoot(), "scripts", "local-dev", "dev_stack.sh")
		if dryRun {
			fmt.Printf("[Dry-Run] Would invoke %s\n", script)
		}
	case ProfileK8sK3s:
		script := filepath.Join(repoRoot(), "scripts", "k8s", "install_k3s.sh")
		if dryRun {
			fmt.Printf("[Dry-Run] Would invoke %s\n", script)
		}
	}

	return nil
}

func boolString(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func checksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func fileUnchanged(path string, content []byte) (bool, error) {
	current, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return checksum(current) == checksum(content), nil
}

func writeFile(path string, content []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, content, mode)
}
