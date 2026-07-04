package config

import "time"

// LifecycleShutdownTimeout returns SHUTDOWN_TIMEOUT_MS for binaries that do not load the full Config.
func LifecycleShutdownTimeout() time.Duration {
	return time.Duration(getEnvInt("SHUTDOWN_TIMEOUT_MS", 15000)) * time.Millisecond
}

// LifecycleWaitTimeout returns WAIT_TIMEOUT_MS for binaries that do not load the full Config.
func LifecycleWaitTimeout() time.Duration {
	return time.Duration(getEnvInt("WAIT_TIMEOUT_MS", 5000)) * time.Millisecond
}
