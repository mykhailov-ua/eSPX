package logcompactor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const s3MetadataSHA256Key = "content-sha256"

// S3Config holds cloud tier store settings.
type S3Config struct {
	Region         string
	Bucket         string
	HotPrefix      string
	WarmPrefix     string
	ScratchDir     string
	Endpoint       string
	ForcePathStyle bool
}

// S3TierStore syncs hot segments from S3 into a scratch filesystem for compaction.
type S3TierStore struct {
	cfg    S3Config
	client *s3.Client
	local  *LocalTierStore
}

// NewS3TierStore builds an S3-backed tier store with local scratch for compaction.
func NewS3TierStore(ctx context.Context, cfg S3Config) (*S3TierStore, error) {
	if cfg.Region == "" || cfg.Bucket == "" {
		return nil, ErrCloudConfigIncomplete
	}
	if cfg.ScratchDir == "" {
		cfg.ScratchDir = "/var/lib/espx/log-compactor/scratch"
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

	hotDir := filepath.Join(cfg.ScratchDir, "hot")
	warmDir := filepath.Join(cfg.ScratchDir, "warm")
	if err := os.MkdirAll(hotDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(warmDir, 0o755); err != nil {
		return nil, err
	}

	return &S3TierStore{
		cfg:    cfg,
		client: client,
		local:  NewLocalTierStore(hotDir, warmDir),
	}, nil
}

// ListHot syncs eligible hot objects from S3 then lists the scratch directory.
func (store *S3TierStore) ListHot(ctx context.Context, olderThan time.Time) ([]TierObject, error) {
	if err := store.syncHotFromS3(ctx, olderThan); err != nil {
		return nil, err
	}
	return store.local.ListHot(ctx, olderThan)
}

func (store *S3TierStore) syncHotFromS3(ctx context.Context, olderThan time.Time) error {
	prefix := store.hotObjectPrefix()
	paginator := s3.NewListObjectsV2Paginator(store.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(store.cfg.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list s3 hot objects: %w", err)
		}
		for _, object := range page.Contents {
			if object.Key == nil || object.LastModified == nil {
				continue
			}
			key := strings.TrimPrefix(*object.Key, prefix)
			if key == "" || !isHotSegmentName(filepath.Base(key)) {
				continue
			}
			if !object.LastModified.Before(olderThan) {
				continue
			}
			localPath := filepath.Join(store.local.SourceDir, filepath.Base(key))
			if _, err := os.Stat(localPath); err == nil {
				continue
			}
			if err := store.downloadObject(ctx, *object.Key, localPath); err != nil {
				return err
			}
			if err := os.Chtimes(localPath, object.LastModified.UTC(), object.LastModified.UTC()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (store *S3TierStore) downloadObject(ctx context.Context, objectKey, destPath string) error {
	output, err := store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(store.cfg.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("get s3 object %q: %w", objectKey, err)
	}
	defer output.Body.Close()

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	tmpPath := destPath + warmTmpSuffix
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(file, output.Body); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, destPath)
}

func (store *S3TierStore) uploadFile(ctx context.Context, key, srcPath, sha256 string) error {
	file, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	_, err = store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(store.cfg.Bucket),
		Key:    aws.String(key),
		Body:   file,
		Metadata: map[string]string{
			s3MetadataSHA256Key: sha256,
		},
		ContentLength: aws.Int64(info.Size()),
	})
	if err != nil {
		return fmt.Errorf("put s3 object %q: %w", key, err)
	}
	return nil
}

func (store *S3TierStore) deleteObject(ctx context.Context, key string) error {
	_, err := store.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(store.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete s3 object %q: %w", key, err)
	}
	return nil
}

func (store *S3TierStore) hotObjectPrefix() string {
	prefix := strings.Trim(store.cfg.HotPrefix, "/")
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

func (store *S3TierStore) warmObjectPrefix() string {
	prefix := strings.Trim(store.cfg.WarmPrefix, "/")
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

func (store *S3TierStore) hotObjectKey(name string) string {
	return store.hotObjectPrefix() + name
}

func (store *S3TierStore) warmObjectKey(name string) string {
	return store.warmObjectPrefix() + name
}

// WriteWarm delegates to scratch storage and uploads warm output to S3.
func (store *S3TierStore) WriteWarm(ctx context.Context, destKey string, plaintext []byte, meta CompactionMeta) error {
	if err := store.local.WriteWarm(ctx, destKey, plaintext, meta); err != nil {
		return err
	}
	return store.uploadWarmArtifacts(ctx, destKey, meta.DestSHA256)
}

// WriteWarmFromFile writes warm output locally and uploads to S3.
func (store *S3TierStore) WriteWarmFromFile(ctx context.Context, destKey, filteredPath string, meta CompactionMeta) (string, error) {
	destSHA, err := store.local.WriteWarmFromFile(ctx, destKey, filteredPath, meta)
	if err != nil {
		return "", err
	}
	if err := store.uploadWarmArtifacts(ctx, destKey, destSHA); err != nil {
		store.local.RemoveWarmArtifacts(destKey)
		return "", err
	}
	return destSHA, nil
}

func (store *S3TierStore) uploadWarmArtifacts(ctx context.Context, destKey, sha256 string) error {
	warmPath := filepath.Join(store.local.WarmDir, destKey)
	if err := store.uploadFile(ctx, store.warmObjectKey(destKey), warmPath, sha256); err != nil {
		return err
	}
	metaPath := strings.TrimSuffix(warmPath, ".zst") + ".meta.json"
	metaDigest, err := computeFileDigest(metaPath)
	if err != nil {
		return err
	}
	metaKey := store.warmObjectKey(strings.TrimSuffix(destKey, ".zst") + ".meta.json")
	return store.uploadFile(ctx, metaKey, metaPath, metaDigest.SHA256)
}

// RemoveHot deletes the scratch copy and the S3 hot object.
func (store *S3TierStore) RemoveHot(ctx context.Context, obj TierObject) error {
	if err := store.local.RemoveHot(ctx, obj); err != nil {
		return err
	}
	hotKey := hotKeyFromCompacting(obj.Key)
	return store.deleteObject(ctx, store.hotObjectKey(hotKey))
}

// ClaimHot claims a scratch hot segment for compaction.
func (store *S3TierStore) ClaimHot(ctx context.Context, obj TierObject) (TierObject, error) {
	return store.local.ClaimHot(ctx, obj)
}

// RollbackHot restores a claimed scratch segment.
func (store *S3TierStore) RollbackHot(ctx context.Context, obj TierObject) error {
	return store.local.RollbackHot(ctx, obj)
}

// ListStuckCompacting returns claimed scratch segments left by a crash.
func (store *S3TierStore) ListStuckCompacting(ctx context.Context) ([]TierObject, error) {
	return store.local.ListStuckCompacting(ctx)
}

// RemoveCompacting deletes a claimed scratch segment and its S3 hot object.
func (store *S3TierStore) RemoveCompacting(ctx context.Context, obj TierObject) error {
	hotKey := hotKeyFromCompacting(obj.Key)
	if err := store.local.RemoveCompacting(ctx, obj); err != nil {
		return err
	}
	return store.deleteObject(ctx, store.hotObjectKey(hotKey))
}

// RemoveWarmArtifacts deletes incomplete warm scratch and S3 temp artifacts.
func (store *S3TierStore) RemoveWarmArtifacts(destKey string) {
	store.local.RemoveWarmArtifacts(destKey)
}

// LocalScratch exposes the scratch LocalTierStore for cold-tier rollup.
func (store *S3TierStore) LocalScratch() *LocalTierStore {
	return store.local
}

// WarmMetaFromS3 downloads warm compaction metadata for ops tooling.
func (store *S3TierStore) WarmMetaFromS3(ctx context.Context, destKey string) (CompactionMeta, error) {
	metaKey := store.warmObjectKey(strings.TrimSuffix(destKey, ".zst") + ".meta.json")
	output, err := store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(store.cfg.Bucket),
		Key:    aws.String(metaKey),
	})
	if err != nil {
		return CompactionMeta{}, err
	}
	defer output.Body.Close()

	var meta CompactionMeta
	if err := json.NewDecoder(output.Body).Decode(&meta); err != nil {
		return CompactionMeta{}, err
	}
	return meta, nil
}
