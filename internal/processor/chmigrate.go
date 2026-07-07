package processor

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed migrations/*.sql
var clickHouseMigrations embed.FS

// ApplyClickHouseMigrations runs idempotent processor analytics DDL on startup.
func ApplyClickHouseMigrations(ctx context.Context, conn driver.Conn) error {
	entries, err := fs.Glob(clickHouseMigrations, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("list clickhouse migrations: %w", err)
	}
	sort.Strings(entries)

	for _, path := range entries {
		body, err := clickHouseMigrations.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", path, err)
		}
		for stmt := range splitClickHouseStatements(string(body)) {
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("apply migration %s: %w", path, err)
			}
		}
		slog.Info("applied clickhouse migration", "file", path)
	}
	return nil
}

func splitClickHouseStatements(sql string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, part := range strings.Split(sql, ";") {
			lines := strings.Split(part, "\n")
			filtered := make([]string, 0, len(lines))
			for _, line := range lines {
				trim := strings.TrimSpace(line)
				if trim == "" || strings.HasPrefix(trim, "--") {
					continue
				}
				if strings.HasPrefix(strings.ToUpper(trim), "USE ") {
					continue
				}
				filtered = append(filtered, line)
			}
			stmt := strings.TrimSpace(strings.Join(filtered, "\n"))
			if stmt == "" {
				continue
			}
			if !yield(stmt) {
				return
			}
		}
	}
}
