package installer

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	healthProbeTimeout = time.Second
	backupDirName      = ".espx/backup"
)

// BinaryDeploy describes a single service binary swap with health verification.
type BinaryDeploy struct {
	Service    string
	SourcePath string
	TargetPath string
	HealthURL  string
	Version    string
}

// backupRoot returns ~/.espx/backup or $ESPX_INSTALL_ROOT/.espx/backup for tests.
func backupRoot() string {
	root := installRoot()
	if root != "" {
		return filepath.Join(root, backupDirName)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return backupDirName
	}
	return filepath.Join(home, backupDirName)
}

func backupPath(service, version string) string {
	name := service
	if version != "" {
		name = fmt.Sprintf("%s-%s", service, version)
	}
	return filepath.Join(backupRoot(), name)
}

// currentBackupMarker stores the path of the last good backup for rollback-guard.
func currentBackupMarker(service string) string {
	return filepath.Join(backupRoot(), service+".current")
}

// BackupBinary copies the installed binary to .espx/backup/<service>-<version>.
func BackupBinary(service, targetPath, version string) (string, error) {
	if _, err := os.Stat(targetPath); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	dest := backupPath(service, version)
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return "", err
	}
	if err := copyFile(targetPath, dest); err != nil {
		return "", fmt.Errorf("backup %s: %w", service, err)
	}
	if err := os.WriteFile(currentBackupMarker(service), []byte(dest), 0644); err != nil {
		return "", err
	}
	return dest, nil
}

// RunHealthProbe executes <binary> --health-probe <url> with a 1 s deadline.
func RunHealthProbe(binaryPath, healthURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), healthProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binaryPath, "--health-probe", healthURL)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("health probe failed: %w (%s)", err, out)
	}
	return nil
}

// DeployBinary backs up, replaces, and probes. Rolls back on probe failure.
func (d *BinaryDeploy) DeployBinary() error {
	if d.Service == "" || d.SourcePath == "" || d.TargetPath == "" {
		return fmt.Errorf("service, source, and target paths are required")
	}
	if _, err := os.Stat(d.SourcePath); err != nil {
		return fmt.Errorf("source binary: %w", err)
	}

	backup, err := BackupBinary(d.Service, d.TargetPath, d.Version)
	if err != nil {
		return err
	}

	if err := copyFile(d.SourcePath, d.TargetPath); err != nil {
		return fmt.Errorf("install %s: %w", d.Service, err)
	}
	if err := os.Chmod(d.TargetPath, 0755); err != nil {
		return err
	}

	if d.HealthURL == "" {
		return nil
	}
	if err := RunHealthProbe(d.TargetPath, d.HealthURL); err != nil {
		if backup != "" {
			_ = copyFile(backup, d.TargetPath)
			_ = os.Chmod(d.TargetPath, 0755)
		}
		return fmt.Errorf("deploy rolled back: %w", err)
	}
	return nil
}

// RollbackService restores the binary from the last backup marker.
func RollbackService(service, targetPath string) error {
	marker := currentBackupMarker(service)
	data, err := os.ReadFile(marker)
	if err != nil {
		return fmt.Errorf("no backup marker for %s: %w", service, err)
	}
	backup := string(data)
	if err := copyFile(backup, targetPath); err != nil {
		return fmt.Errorf("rollback %s: %w", service, err)
	}
	return os.Chmod(targetPath, 0755)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
