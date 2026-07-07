package management

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// WarmCampaignBudget writes budget:campaign:{id} from Postgres remaining spend without registry reload.
func (s *Service) WarmCampaignBudget(ctx context.Context, campaignID uuid.UUID) (int64, error) {
	rdb := s.getRDB(campaignID)
	if rdb == nil {
		return 0, fmt.Errorf("no redis client available")
	}
	worker := NewOutboxWorker(s)
	remaining, err := worker.campaignRemainingBudget(ctx, campaignID)
	if err != nil {
		return 0, err
	}
	if remaining <= 0 {
		return 0, nil
	}
	_, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		return worker.setCampaignBudgetRemaining(ctx, pipe, campaignID.String(), campaignID, 0)
	})
	if err != nil {
		return 0, err
	}
	return remaining, nil
}
