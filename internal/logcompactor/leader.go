package logcompactor

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

// FileLeaderLock provides single-writer leader election via POSIX flock.
type FileLeaderLock struct {
	path string
	file *os.File
}

// NewFileLeaderLock returns a lock backed by path (created if missing).
func NewFileLeaderLock(path string) *FileLeaderLock {
	return &FileLeaderLock{path: path}
}

// TryAcquire attempts a non-blocking exclusive flock.
func (lock *FileLeaderLock) TryAcquire() (bool, error) {
	if lock.file != nil {
		return true, nil
	}

	file, err := os.OpenFile(lock.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return false, err
	}

	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if err == syscall.EWOULDBLOCK {
			leaderHeld.Set(0)
			return false, nil
		}
		return false, fmt.Errorf("flock %s: %w", lock.path, err)
	}

	lock.file = file
	leaderHeld.Set(1)
	return true, nil
}

// Release drops the exclusive flock.
func (lock *FileLeaderLock) Release() error {
	if lock.file == nil {
		leaderHeld.Set(0)
		return nil
	}
	err := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	closeErr := lock.file.Close()
	lock.file = nil
	leaderHeld.Set(0)
	if err != nil {
		return err
	}
	return closeErr
}

// Path returns the lock file path.
func (lock *FileLeaderLock) Path() string {
	return lock.path
}

// leaderWaitBackoff returns how long a standby instance sleeps before retrying.
func leaderWaitBackoff(interval time.Duration) time.Duration {
	if interval <= 0 {
		return time.Second
	}
	if interval > 5*time.Second {
		return 5 * time.Second
	}
	return interval
}
