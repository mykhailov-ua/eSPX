package dedup

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadRoutingEpoch returns the latest control_plane_epochs id for source_epoch (M4-11).
func LoadRoutingEpoch(ctx context.Context, pool *pgxpool.Pool) uint32 {
	if pool == nil {
		return 0
	}
	var epoch int64
	err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(epoch_id), 0) FROM control_plane_epochs`).Scan(&epoch)
	if err != nil || epoch < 0 {
		return 0
	}
	return uint32(epoch)
}
