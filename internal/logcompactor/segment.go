package logcompactor

import (
	"os"
	"strings"

	"espx/pkg/logger"
)

const readySuffix = ".log.zst.ready"

// compactStats tracks how many records were scanned vs kept during one compaction pass.
type compactStats struct {
	OriginalCount int64
	KeptCount     int64
}

// readSegmentBytes loads and optionally decrypts a rotated audit segment from disk.
func readSegmentBytes(path string, decryptKey []byte) ([]byte, error) {
	if strings.HasSuffix(path, readySuffix) {
		if len(decryptKey) == 0 {
			passphrase := os.Getenv("LOG_ENCRYPTION_KEY")
			if passphrase == "" {
				passphrase = "default-espx-logger-fallback-passphrase-change-me"
			}
			decryptKey = logger.DeriveKey(passphrase)
		}
		return logger.DecryptSegment(path, decryptKey)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}
