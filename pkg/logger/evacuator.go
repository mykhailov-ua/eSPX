package logger

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
)

// S3Uploader is the interface for streaming compressed log segments to object storage.
type S3Uploader interface {
	UploadMultipart(key string, filePath string) (string, error)
}

// StartEvacuator runs the background archive/evacuation loop.
func (l *Logger) StartEvacuator(uploader S3Uploader) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.closeChan:
			l.EvacuatePendingSegments(uploader)
			return
		case <-ticker.C:
			l.EvacuatePendingSegments(uploader)
		}
	}
}

// EvacuatePendingSegments scans the log directory for rotated segments,
// compresses them, uploads to S3, and removes local files on success.
func (l *Logger) EvacuatePendingSegments(uploader S3Uploader) {
	pattern := filepath.Join(l.cfg.LogDir, "segment_*.log")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return
	}

	for _, srcPath := range matches {
		dstPath := srcPath + ".zst"

		err := l.compressSegment(srcPath, dstPath)
		if err != nil {
			_ = os.Remove(dstPath)
			continue
		}

		localMD5, err := calculateMD5(dstPath)
		if err != nil {
			_ = os.Remove(dstPath)
			continue
		}

		key := filepath.Base(dstPath)
		etag, err := uploader.UploadMultipart(key, dstPath)
		if err != nil {
			_ = os.Remove(dstPath)
			continue
		}

		if etag != "" && etag != localMD5 {
			_ = os.Remove(dstPath)
			continue
		}

		_ = os.Remove(srcPath)
		_ = os.Remove(dstPath)
	}
}

func (l *Logger) compressSegment(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer dst.Close()

	writer, err := zstd.NewWriter(dst, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	defer writer.Close()

	_, err = io.Copy(writer, src)
	return err
}

func calculateMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// MockS3Uploader is a memory-backed uploader for testing.
type MockS3Uploader struct {
	Uploads   map[string]string
	FailRoute bool
}

func NewMockS3Uploader() *MockS3Uploader {
	return &MockS3Uploader{
		Uploads: make(map[string]string),
	}
}

func (m *MockS3Uploader) UploadMultipart(key string, filePath string) (string, error) {
	if m.FailRoute {
		return "", fmt.Errorf("simulated S3 connectivity failure")
	}

	md5Sum, err := calculateMD5(filePath)
	if err != nil {
		return "", err
	}

	m.Uploads[key] = md5Sum
	return md5Sum, nil
}
