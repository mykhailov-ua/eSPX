package testutil

import (
	"path/filepath"
	"runtime"
)

func ModuleRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

func AdsMigrationsDir() string {
	return filepath.Join(ModuleRoot(), "internal", "ads", "migrations")
}

func AuthMigrationsDir() string {
	return filepath.Join(ModuleRoot(), "internal", "auth", "migrations")
}

func PaymentMigrationsDir() string {
	return filepath.Join(ModuleRoot(), "internal", "payment", "migrations")
}
