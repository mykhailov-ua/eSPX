package management

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type NginxConfigWorker struct {
	svc        *Service
	exportPath string
	reloadCmd  string
}

func NewNginxConfigWorker(svc *Service, exportPath string) *NginxConfigWorker {
	return &NginxConfigWorker{
		svc:        svc,
		exportPath: exportPath,
		reloadCmd:  "nginx -s reload",
	}
}

func (w *NginxConfigWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.ExportAndReload(ctx); err != nil {
				slog.Error("nginx export failed", "error", err)
			}
		}
	}
}

func (w *NginxConfigWorker) ExportAndReload(ctx context.Context) error {
	rdb := w.svc.rdbs[0]

	manual, err := rdb.SMembers(ctx, "blacklist:manual").Result()
	if err != nil {
		return fmt.Errorf("failed to fetch manual blacklist: %w", err)
	}
	if err := w.writeDenyFile("manual.conf", manual); err != nil {
		return err
	}

	auto, err := rdb.SMembers(ctx, "blacklist:auto").Result()
	if err != nil {
		return fmt.Errorf("failed to fetch auto blacklist: %w", err)
	}
	if err := w.writeDenyFile("auto.conf", auto); err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", w.reloadCmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to reload nginx: %w, output: %s", err, string(out))
	}

	slog.Info("nginx blacklist exported and reloaded", "manual_count", len(manual), "auto_count", len(auto))
	return nil
}

func (w *NginxConfigWorker) writeDenyFile(filename string, ips []string) error {
	if err := os.MkdirAll(w.exportPath, 0755); err != nil {
		return err
	}

	path := filepath.Join(w.exportPath, filename)
	var sb strings.Builder
	for _, ip := range ips {
		if ip == "" {
			continue
		}
		sb.WriteString("deny ")
		sb.WriteString(ip)
		sb.WriteString(";\n")
	}

	return os.WriteFile(path, []byte(sb.String()), 0644)
}
