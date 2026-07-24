package ingestion

import (
	"context"
	"fmt"

	db "espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CampaignRoutingRepo manages per-campaign elastic triplet routing (M2).
type CampaignRoutingRepo struct {
	pool *pgxpool.Pool
}

// NewCampaignRoutingRepo constructs a campaign routing repository.
func NewCampaignRoutingRepo(pool *pgxpool.Pool) *CampaignRoutingRepo {
	return &CampaignRoutingRepo{pool: pool}
}

// UpsertCampaignRouting writes or updates triplet home for a campaign.
func (r *CampaignRoutingRepo) UpsertCampaignRouting(
	ctx context.Context,
	campaignID uuid.UUID,
	homeSlot int16,
	primaryA, primaryB, reserve int16,
	routingEpoch int64,
	hEma, cEma float64,
) (db.CampaignRouting, error) {
	if r.pool == nil {
		return db.CampaignRouting{}, fmt.Errorf("campaign routing repo: nil pool")
	}
	return db.New(r.pool).UpsertCampaignRouting(ctx, db.UpsertCampaignRoutingParams{
		CampaignID:    ToUUID(campaignID),
		HomeSlot:      homeSlot,
		PrimaryAShard: primaryA,
		PrimaryBShard: primaryB,
		ReserveShard:  reserve,
		RoutingEpoch:  routingEpoch,
		HEma:          hEma,
		CEma:          cEma,
	})
}

// GetCampaignRouting returns routing row for a campaign when present.
func (r *CampaignRoutingRepo) GetCampaignRouting(ctx context.Context, campaignID uuid.UUID) (db.CampaignRouting, error) {
	if r.pool == nil {
		return db.CampaignRouting{}, fmt.Errorf("campaign routing repo: nil pool")
	}
	return db.New(r.pool).GetCampaignRouting(ctx, ToUUID(campaignID))
}

// BumpGlobalRoutingEpoch increments the global routing epoch and returns the new value.
func (r *CampaignRoutingRepo) BumpGlobalRoutingEpoch(ctx context.Context) (db.BumpGlobalRoutingEpochRow, error) {
	if r.pool == nil {
		return db.BumpGlobalRoutingEpochRow{}, fmt.Errorf("campaign routing repo: nil pool")
	}
	return db.New(r.pool).BumpGlobalRoutingEpoch(ctx)
}

// GetGlobalRoutingEpoch returns the current global routing epoch and active slot map version.
func (r *CampaignRoutingRepo) GetGlobalRoutingEpoch(ctx context.Context) (db.GetGlobalRoutingEpochRow, error) {
	if r.pool == nil {
		return db.GetGlobalRoutingEpochRow{}, fmt.Errorf("campaign routing repo: nil pool")
	}
	return db.New(r.pool).GetGlobalRoutingEpoch(ctx)
}

// HomeSlotForCampaign returns crc32 slot index for a campaign UUID.
func HomeSlotForCampaign(id uuid.UUID) int16 {
	return int16(CampaignSlotIndex(id))
}
