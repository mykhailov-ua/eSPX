// Command admin is the binary entrypoint for the internal developer CLI (cmd/admin/cmd).
package main

import (
	"log/slog"
	"os"

	"espx/cmd/admin/cmd"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := cmd.Execute(); err != nil {
		slog.Error("admin command failed", "error", err)
		os.Exit(1)
	}
}
