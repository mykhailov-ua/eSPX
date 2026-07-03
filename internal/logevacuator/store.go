package logevacuator

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	metadataSHA256Key = "content-sha256"
	defaultPartSize   = 8 * 1024 * 1024
)

// ObjectHead describes an existing object used for exactly-once idempotency checks.
type ObjectHead struct {
	Exists bool
	SHA256 string
	ETag   string
	Size   int64
}

// ObjectStore uploads rotated log segments and supports digest-based idempotency.
type ObjectStore interface {
	HeadObject(ctx context.Context, key string) (ObjectHead, error)
	PutObject(ctx context.Context, key string, filePath string, digests fileDigests) error
}

// S3Store uploads segments to S3 with single-part or multipart uploads and ETag verification.
type S3Store struct {
	client             *s3.Client
	bucket             string
	prefix             string
	multipartThreshold int64
}

// S3Config configures the AWS S3 object store backend.
type S3Config struct {
	Region             string
	Bucket             string
	Prefix             string
	Endpoint           string
	ForcePathStyle     bool
	MultipartThreshold int64
}

// NewS3Store builds an S3-backed ObjectStore from AWS SDK configuration.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Region == "" {
		return nil, ErrRegionRequired
	}
	if cfg.Bucket == "" {
		return nil, ErrBucketRequired
	}

	threshold := cfg.MultipartThreshold
	if threshold <= 0 {
		threshold = defaultPartSize
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		options.UsePathStyle = cfg.ForcePathStyle
	})

	return &S3Store{
		client:             client,
		bucket:             cfg.Bucket,
		prefix:             strings.Trim(cfg.Prefix, "/"),
		multipartThreshold: threshold,
	}, nil
}

// HeadObject returns stored digest metadata for idempotent retries.
func (store *S3Store) HeadObject(ctx context.Context, key string) (ObjectHead, error) {
	output, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(store.objectKey(key)),
	})
	if err != nil {
		if isS3NotFound(err) {
			return ObjectHead{}, nil
		}
		return ObjectHead{}, err
	}

	head := ObjectHead{Exists: true}
	if output.ContentLength != nil {
		head.Size = *output.ContentLength
	}
	if output.ETag != nil {
		head.ETag = strings.Trim(*output.ETag, "\"")
	}
	if output.Metadata != nil {
		head.SHA256 = output.Metadata[metadataSHA256Key]
	}

	return head, nil
}

// PutObject uploads the file using single-part or multipart upload and verifies the returned ETag.
func (store *S3Store) PutObject(ctx context.Context, key string, filePath string, digests fileDigests) error {
	if digests.Size < store.multipartThreshold {
		return store.putSinglePart(ctx, key, filePath, digests)
	}
	return store.putMultipart(ctx, key, filePath, digests)
}

func (store *S3Store) putSinglePart(ctx context.Context, key string, filePath string, digests fileDigests) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	output, err := store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(store.bucket),
		Key:           aws.String(store.objectKey(key)),
		Body:          file,
		ContentLength: aws.Int64(digests.Size),
		Metadata: map[string]string{
			metadataSHA256Key: digests.SHA256,
		},
	})
	if err != nil {
		return err
	}

	if output.ETag == nil {
		return ErrETagMismatch
	}
	gotETag := strings.Trim(*output.ETag, "\"")
	if gotETag != digests.MD5 {
		return ErrETagMismatch
	}

	return nil
}

func (store *S3Store) putMultipart(ctx context.Context, key string, filePath string, digests fileDigests) error {
	createOutput, err := store.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(store.objectKey(key)),
		Metadata: map[string]string{
			metadataSHA256Key: digests.SHA256,
		},
	})
	if err != nil {
		return err
	}

	uploadID := createOutput.UploadId
	abort := func() {
		_, _ = store.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(store.bucket),
			Key:      aws.String(store.objectKey(key)),
			UploadId: uploadID,
		})
	}

	file, err := os.Open(filePath)
	if err != nil {
		abort()
		return err
	}
	defer file.Close()

	buffer := copyBuffer()
	completedParts := make([]types.CompletedPart, 0, 8)
	partNumber := int32(1)

	for {
		n, readErr := io.ReadFull(file, buffer)
		if readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			abort()
			return readErr
		}

		partBody := buffer[:n]
		uploadOutput, uploadErr := store.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(store.bucket),
			Key:        aws.String(store.objectKey(key)),
			UploadId:   uploadID,
			PartNumber: aws.Int32(partNumber),
			Body:       bytes.NewReader(partBody),
		})
		if uploadErr != nil {
			abort()
			return uploadErr
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadOutput.ETag,
			PartNumber: aws.Int32(partNumber),
		})
		partNumber++

		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	completeOutput, err := store.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(store.bucket),
		Key:      aws.String(store.objectKey(key)),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		abort()
		return err
	}

	if completeOutput.ETag == nil || *completeOutput.ETag == "" {
		return ErrETagMismatch
	}

	return nil
}

func (store *S3Store) objectKey(key string) string {
	if store.prefix == "" {
		return key
	}
	return store.prefix + "/" + key
}

func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404")
}

// MemoryStore is an in-process object store used by chaos tests to verify digest idempotency without AWS.
type MemoryStore struct {
	objects map[string]memoryObject
}

type memoryObject struct {
	SHA256 string
	ETag   string
	Size   int64
	Data   []byte
}

// NewMemoryStore returns an empty in-memory object store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string]memoryObject)}
}

// HeadObject reports whether the object exists and returns stored digest metadata.
func (store *MemoryStore) HeadObject(_ context.Context, key string) (ObjectHead, error) {
	object, ok := store.objects[key]
	if !ok {
		return ObjectHead{}, nil
	}
	return ObjectHead{
		Exists: true,
		SHA256: object.SHA256,
		ETag:   object.ETag,
		Size:   object.Size,
	}, nil
}

// PutObject stores the file bytes and records digest metadata mirroring S3 ETag semantics.
func (store *MemoryStore) PutObject(_ context.Context, key string, filePath string, digests fileDigests) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	sum := md5.Sum(data)
	etag := hex.EncodeToString(sum[:])
	if etag != digests.MD5 {
		return ErrDigestMismatch
	}

	store.objects[key] = memoryObject{
		SHA256: digests.SHA256,
		ETag:   etag,
		Size:   digests.Size,
		Data:   data,
	}
	return nil
}

// ObjectCount returns the number of stored objects for test assertions.
func (store *MemoryStore) ObjectCount() int {
	return len(store.objects)
}

// ObjectData returns stored bytes for test assertions.
func (store *MemoryStore) ObjectData(key string) []byte {
	return store.objects[key].Data
}
