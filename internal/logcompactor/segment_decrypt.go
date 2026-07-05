package logcompactor

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"

	"espx/pkg/logger"

	"github.com/klauspost/compress/zstd"
)

// decryptedSegmentReader streams plaintext audit records from an encrypted segment file.
type decryptedSegmentReader struct {
	file   *os.File
	aesgcm cipher.AEAD
	decoder *zstd.Decoder
	buf    []byte
	off    int
	done   bool
	header [4]byte
	nonce  [12]byte
}

// openPlaintextSegment opens a hot segment as a plaintext record stream.
func openPlaintextSegment(path string, decryptKey []byte) (io.ReadCloser, error) {
	if strings.HasSuffix(path, readySuffix) {
		if len(decryptKey) == 0 {
			passphrase := os.Getenv("LOG_ENCRYPTION_KEY")
			if passphrase == "" {
				passphrase = "default-espx-logger-fallback-passphrase-change-me"
			}
			decryptKey = logger.DeriveKey(passphrase)
		}
		return openDecryptedSegment(path, decryptKey)
	}
	return os.Open(path)
}

func openDecryptedSegment(path string, key []byte) (*decryptedSegmentReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &decryptedSegmentReader{
		file:    file,
		aesgcm:  aesgcm,
		decoder: decoder,
	}, nil
}

func (reader *decryptedSegmentReader) Read(p []byte) (int, error) {
	for {
		if reader.off < len(reader.buf) {
			n := copy(p, reader.buf[reader.off:])
			reader.off += n
			return n, nil
		}
		if reader.done {
			return 0, io.EOF
		}
		if err := reader.readNextBlock(); err != nil {
			if err == io.EOF {
				reader.done = true
				return 0, io.EOF
			}
			return 0, err
		}
		reader.off = 0
	}
}

func (reader *decryptedSegmentReader) Close() error {
	if reader.decoder != nil {
		reader.decoder.Close()
	}
	if reader.file != nil {
		return reader.file.Close()
	}
	return nil
}

func (reader *decryptedSegmentReader) readNextBlock() error {
	_, err := io.ReadFull(reader.file, reader.header[:])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return io.EOF
	}
	if err != nil {
		return err
	}

	length := binary.BigEndian.Uint32(reader.header[:])
	if length < 12+16 {
		return fmt.Errorf("invalid encrypted block length: %d", length)
	}

	if _, err := io.ReadFull(reader.file, reader.nonce[:]); err != nil {
		return err
	}

	ciphertextLen := length - 12
	ciphertext := make([]byte, ciphertextLen)
	if _, err := io.ReadFull(reader.file, ciphertext); err != nil {
		return err
	}

	plaintext, err := reader.aesgcm.Open(nil, reader.nonce[:], ciphertext, nil)
	if err != nil {
		return err
	}

	decompressed, err := reader.decoder.DecodeAll(plaintext, reader.buf[:0])
	if err != nil {
		return fmt.Errorf("decompress encrypted block: %w", err)
	}
	reader.buf = decompressed
	return nil
}
