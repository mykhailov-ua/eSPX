package ingestion

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// CampaignKeyMigrator copies campaign-scoped Redis keys between shards (cold path only).
type CampaignKeyMigrator struct {
	Catalog *CampaignRedisKeyCatalog
}

func (m *CampaignKeyMigrator) catalog() *CampaignRedisKeyCatalog {
	if m != nil && m.Catalog != nil {
		return m.Catalog
	}
	return DefaultCampaignRedisKeyCatalog
}

// MigrateCampaignKeys idempotently copies budget, quota, fcap, and sync keys from src to dst.
// Returns the number of keys migrated (skipped missing keys are not counted).
func (m *CampaignKeyMigrator) MigrateCampaignKeys(
	ctx context.Context,
	src, dst redis.Cmdable,
	campaignID uuid.UUID,
) (int, error) {
	cat := m.catalog()
	migrated := 0
	for _, key := range cat.FixedKeys(campaignID) {
		ok, err := migrateKey(ctx, src, dst, key)
		if err != nil {
			return migrated, err
		}
		if ok {
			migrated++
		}
	}

	for _, prefix := range cat.PrefixPatterns(campaignID) {
		n, err := migrateKeysByPrefix(ctx, src, dst, prefix)
		if err != nil {
			return migrated, err
		}
		migrated += n
	}
	return migrated, nil
}

// DrainCampaignKeys deletes campaign keys from src after cutover (old shard cleanup).
func (m *CampaignKeyMigrator) DrainCampaignKeys(
	ctx context.Context,
	src redis.Cmdable,
	campaignID uuid.UUID,
) (int, error) {
	cat := m.catalog()
	deleted := 0
	keys := append(cat.FixedKeys(campaignID), cat.SourceOnlyKeys(campaignID)...)
	for _, key := range keys {
		n, err := src.Del(ctx, key).Result()
		if err != nil {
			return deleted, err
		}
		if n > 0 {
			deleted++
		}
	}

	for _, prefix := range cat.PrefixPatterns(campaignID) {
		n, err := deleteKeysByPrefix(ctx, src, prefix)
		if err != nil {
			return deleted, err
		}
		deleted += n
	}
	return deleted, nil
}

func migrateKey(ctx context.Context, src, dst redis.Cmdable, key string) (bool, error) {
	exists, err := src.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if exists == 0 {
		return false, nil
	}
	payload, err := src.Dump(ctx, key).Bytes()
	if err != nil {
		return false, fmt.Errorf("dump %q: %w", key, err)
	}
	ttl, err := src.TTL(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("ttl %q: %w", key, err)
	}
	if ttl < 0 {
		ttl = 0
	}
	if err := dst.RestoreReplace(ctx, key, ttl, string(payload)).Err(); err != nil {
		return false, fmt.Errorf("restore %q: %w", key, err)
	}
	return true, nil
}

func migrateKeysByPrefix(ctx context.Context, src, dst redis.Cmdable, prefix string) (int, error) {
	migrated := 0
	var cursor uint64
	for {
		keys, next, err := src.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return migrated, err
		}
		for _, key := range keys {
			ok, err := migrateKey(ctx, src, dst, key)
			if err != nil {
				return migrated, err
			}
			if ok {
				migrated++
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return migrated, nil
}

func deleteKeysByPrefix(ctx context.Context, src redis.Cmdable, prefix string) (int, error) {
	deleted := 0
	var cursor uint64
	for {
		keys, next, err := src.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return deleted, err
		}
		for _, key := range keys {
			n, err := src.Del(ctx, key).Result()
			if err != nil {
				return deleted, err
			}
			if n > 0 {
				deleted++
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return deleted, nil
}
