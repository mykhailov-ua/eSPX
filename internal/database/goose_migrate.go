package database

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ApplyGooseMigrationsDir runs goose Up sections from *.sql files in dir, in filename order.
func ApplyGooseMigrationsDir(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", dir, err)
	}
	return applyGooseEntries(ctx, pool, entries, func(name string) ([]byte, error) {
		return os.ReadFile(dir + string(os.PathSeparator) + name)
	})
}

// ApplyGooseMigrationsFS runs goose Up sections from *.sql files under root in an fs.FS.
func ApplyGooseMigrationsFS(ctx context.Context, pool *pgxpool.Pool, migrations fs.FS, root string) error {
	entries, err := fs.ReadDir(migrations, root)
	if err != nil {
		return fmt.Errorf("read migrations fs %s: %w", root, err)
	}
	return applyGooseEntries(ctx, pool, entries, func(name string) ([]byte, error) {
		return fs.ReadFile(migrations, root+"/"+name)
	})
}

func applyGooseEntries(
	ctx context.Context,
	pool *pgxpool.Pool,
	entries []fs.DirEntry,
	readFile func(name string) ([]byte, error),
) error {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := readFile(entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		upSQL := GooseUpSQL(string(sqlBytes))
		if strings.TrimSpace(upSQL) == "" {
			continue
		}
		if _, err := pool.Exec(ctx, upSQL); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// GooseUpSQL extracts the goose Up section from a migration file.
func GooseUpSQL(sql string) string {
	parts := strings.Split(sql, "-- +goose Down")
	upPart := parts[0]
	upPart = strings.ReplaceAll(upPart, "-- +goose Up", "")
	upPart = strings.ReplaceAll(upPart, "-- +goose StatementBegin", "")
	upPart = strings.ReplaceAll(upPart, "-- +goose StatementEnd", "")
	return upPart
}
