package installer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"espx/internal/licensing"
)

func RunLicense(cmd string) error {
	switch cmd {
	case "install":
		return installLicenseFromEnv()
	case "activate":
		return activateLicense()
	case "status":
		return licenseStatus()
	default:
		return fmt.Errorf("unknown license command: %s", cmd)
	}
}

func installLicenseFromEnv() error {
	src := os.Getenv("ESPX_LICENSE_SRC")
	if src == "" {
		return fmt.Errorf("set ESPX_LICENSE_SRC to the license JWT file path")
	}

	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(licensePath()), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(licensePath(), data, 0600); err != nil {
		return err
	}

	fmt.Printf("License installed to %s\n", licensePath())
	return nil
}

func activateLicense() error {
	serverURL := os.Getenv("ESPX_LICENSE_SERVER")
	licenseKey := os.Getenv("ESPX_LICENSE_KEY")
	deploymentID := os.Getenv("ESPX_DEPLOYMENT_ID")
	fingerprint := os.Getenv("ESPX_DEPLOYMENT_FINGERPRINT")

	if serverURL == "" || licenseKey == "" || deploymentID == "" {
		return fmt.Errorf("set ESPX_LICENSE_SERVER, ESPX_LICENSE_KEY, and ESPX_DEPLOYMENT_ID")
	}

	client := licensing.NewLicenseClient(serverURL, licenseKey, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	token, err := client.Activate(ctx, deploymentID, fingerprint)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(licensePath()), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(licensePath(), []byte(token), 0600); err != nil {
		return err
	}

	fmt.Printf("License activated and saved to %s\n", licensePath())
	return nil
}

func licenseStatus() error {
	data, err := os.ReadFile(licensePath())
	if err != nil {
		return fmt.Errorf("read license: %w", err)
	}

	claims, err := licensing.DecodeUnverified(string(data))
	if err != nil {
		return err
	}

	state := licensing.DetermineState(claims, time.Now(), false)
	fmt.Printf("License status: %s (deployment_id=%s plan=%s valid_until=%s)\n",
		state, claims.DeploymentID, claims.Plan, claims.ValidUntil.Format(time.RFC3339))
	return nil
}
