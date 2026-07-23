package management

import (
	"context"

	"espx/internal/dedup"
)

func (s *Service) dedupAdapter() *dedup.Adapter {
	if s == nil || s.pool == nil || s.cfg == nil {
		return nil
	}
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	epoch := dedup.LoadRoutingEpoch(ctx, s.pool)
	return dedup.NewAdapter(s.pool, s.cfg.RegionCode, epoch)
}
