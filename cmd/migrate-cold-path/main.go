// Command migrate-cold-path applies tracked Postgres migrations for local k3s cold-path bring-up.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"strings"

	"espx/internal/database"
	"espx/internal/notifier"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	only := flag.String("only", "", "comma-separated migration sets: ads,auth,billing,notifier")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		slog.Error("DB_DSN is required")
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := database.Connect(ctx, dsn, 4, 1)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := applyColdPathMigrations(ctx, pool, parseOnly(*only)); err != nil {
		slog.Error("cold-path migration failed", "error", err)
		os.Exit(1)
	}

	slog.Info("cold-path migrations complete")
}

func applyColdPathMigrations(ctx context.Context, pool *pgxpool.Pool, only map[string]bool) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}

	runAll := len(only) == 0
	run := func(name string) bool { return runAll || only[name] }

	if run("ads") || run("auth") || run("billing") {
		for _, item := range []struct {
			name string
			rel  string
		}{
			{name: "ads", rel: "internal/ads/migrations"},
			{name: "auth", rel: "internal/auth/migrations"},
			{name: "billing", rel: "internal/billing/migrations"},
		} {
			if !run(item.name) {
				continue
			}
			dir := root + "/" + item.rel
			slog.Info("applying migrations", "dir", item.rel)
			if err := database.ApplyTrackedGooseMigrationsDir(ctx, pool, dir); err != nil {
				return err
			}
		}
	}

	if run("notifier") {
		slog.Info("applying notifier schema migrations")
		if err := notifier.ApplyMigrations(ctx, pool); err != nil {
			return err
		}
	}

	return nil
}

func parseOnly(raw string) map[string]bool {
	out := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out[part] = true
		}
	}
	return out
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(wd + "/go.mod"); err == nil {
		return wd, nil
	}
	return "", errors.New("run from repository root")
}
