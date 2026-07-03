package logevacuator

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// fileDigests holds content hashes used for S3 ETag verification and checkpoint idempotency.
type fileDigests struct {
	SHA256 string
	MD5    string
	Size   int64
}

// computeFileDigests reads the file once and returns SHA-256 and MD5 digests for upload verification.
func computeFileDigests(path string) (fileDigests, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileDigests{}, err
	}
	defer file.Close()

	sha := sha256.New()
	md5Hash := md5.New()
	reader := io.TeeReader(file, md5Hash)

	written, err := io.CopyBuffer(sha, reader, copyBuffer())
	if err != nil {
		return fileDigests{}, err
	}

	return fileDigests{
		SHA256: hex.EncodeToString(sha.Sum(nil)),
		MD5:    fmt.Sprintf("%x", md5Hash.Sum(nil)),
		Size:   written,
	}, nil
}
