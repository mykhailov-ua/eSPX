package logcompactor

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

// fileDigest holds a content hash for checkpoint idempotency.
type fileDigest struct {
	SHA256 string
	Size   int64
}

func computeFileDigest(path string) (fileDigest, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileDigest{}, err
	}
	defer file.Close()

	sha := sha256.New()
	written, err := io.Copy(sha, file)
	if err != nil {
		return fileDigest{}, err
	}

	return fileDigest{
		SHA256: hex.EncodeToString(sha.Sum(nil)),
		Size:   written,
	}, nil
}
