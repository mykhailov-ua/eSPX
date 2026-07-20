package installer

import (
	"os"
	"path/filepath"
)

const (
	defaultSecretsPath = "/etc/espx/secrets.env"
	defaultLicensePath = "/etc/espx/license.jwt"
)

func repoRoot() string {
	if root := os.Getenv("ESPX_REPO_ROOT"); root != "" {
		return root
	}
	if root := os.Getenv("ROOT"); root != "" {
		return root
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "."
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return cwd
		}
		dir = parent
	}
}

func installRoot() string {
	if root := os.Getenv("ESPX_INSTALL_ROOT"); root != "" {
		return root
	}
	return ""
}

func secretsPath() string {
	if root := installRoot(); root != "" {
		return filepath.Join(root, "etc/espx/secrets.env")
	}
	return defaultSecretsPath
}

func licensePath() string {
	if root := installRoot(); root != "" {
		return filepath.Join(root, "etc/espx/license.jwt")
	}
	return defaultLicensePath
}

func systemdUnitPath(name string) string {
	if root := installRoot(); root != "" {
		return filepath.Join(root, "etc/systemd/system", name)
	}
	return filepath.Join("/etc/systemd/system", name)
}

func packagesYAMLPath() string {
	return filepath.Join(repoRoot(), "deploy", "installer", "packages.yaml")
}

func checkDepsScript() string {
	return filepath.Join(repoRoot(), "scripts", "ci", "check_deps.sh")
}
