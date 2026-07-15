-- Campaign templates

-- name: CreateCampaignTemplate :one
INSERT INTO campaign_templates (
    id, customer_id, name, budget_limit, pacing_mode, daily_budget, timezone,
    freq_limit, freq_window, target_countries, brand_id, daypart_hours
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
RETURNING *;

-- name: GetCampaignTemplate :one
SELECT * FROM campaign_templates WHERE id = $1;

-- name: ListCampaignTemplates :many
SELECT * FROM campaign_templates
WHERE customer_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountCampaignTemplates :one
SELECT COUNT(*) FROM campaign_templates WHERE customer_id = $1;

-- Campaign schedule / lifecycle

-- name: UpdateCampaignSchedule :one
UPDATE campaigns
SET start_at = $2,
    end_at = $3,
    daypart_hours = $4,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: ListScheduledCampaigns :many
SELECT * FROM campaigns
WHERE deleted_at IS NULL
  AND status IN ('ACTIVE', 'PAUSED')
  AND (start_at IS NOT NULL OR end_at IS NOT NULL)
ORDER BY updated_at ASC
LIMIT $1;

-- name: ClaimScheduledCampaignForUpdate :one
SELECT * FROM campaigns
WHERE deleted_at IS NULL
  AND status IN ('ACTIVE', 'PAUSED')
  AND (start_at IS NOT NULL OR end_at IS NOT NULL)
ORDER BY updated_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- name: PauseCampaign :one
UPDATE campaigns
SET status = 'PAUSED',
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND status IN ('ACTIVE', 'EXHAUSTED')
RETURNING *;

-- name: ResumeCampaign :one
UPDATE campaigns
SET status = 'ACTIVE',
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND status = 'PAUSED'
RETURNING *;

-- Brand creatives

-- name: CreateBrandCreative :one
INSERT INTO brand_creatives (id, brand_id, name, landing_url, weight, status)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetBrandCreative :one
SELECT * FROM brand_creatives WHERE id = $1;

-- name: ListBrandCreatives :many
SELECT * FROM brand_creatives
WHERE brand_id = $1
ORDER BY created_at ASC;

-- name: UpdateBrandCreative :one
UPDATE brand_creatives
SET name = $2,
    landing_url = $3,
    weight = $4,
    status = $5,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: DeleteBrandCreative :exec
DELETE FROM brand_creatives WHERE id = $1;

-- name: ListActiveBrandCreatives :many
SELECT * FROM brand_creatives
WHERE brand_id = $1 AND status = 'ACTIVE'
ORDER BY created_at ASC;

-- name: ListDistinctBrandsWithActiveCreatives :many
SELECT DISTINCT brand_id FROM brand_creatives WHERE status = 'ACTIVE';

-- name: ListCampaignIDsByBrand :many
SELECT id FROM campaigns
WHERE brand_id = $1 AND deleted_at IS NULL AND status IN ('ACTIVE', 'PAUSED');
