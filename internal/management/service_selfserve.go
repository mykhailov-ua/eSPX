package management

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// EnforceSelfServeCreateLimits guards tenant quotas before a new campaign reserves budget.
func (s *Service) EnforceSelfServeCreateLimits(ctx context.Context, customerID uuid.UUID, budgetMicro int64) error {
	if s.cfg == nil {
		return nil
	}
	if s.cfg.SelfServeBudgetMinMicro > 0 && budgetMicro < s.cfg.SelfServeBudgetMinMicro {
		return fmt.Errorf("%w: minimum %d micro", ErrSelfServeBudgetOutOfRange, s.cfg.SelfServeBudgetMinMicro)
	}
	if s.cfg.SelfServeBudgetMaxMicro > 0 && budgetMicro > s.cfg.SelfServeBudgetMaxMicro {
		return fmt.Errorf("%w: maximum %d micro", ErrSelfServeBudgetOutOfRange, s.cfg.SelfServeBudgetMaxMicro)
	}

	var active int64
	err := s.GetPool().QueryRow(ctx, `
		SELECT COUNT(*) FROM campaigns
		WHERE customer_id = $1 AND status = 'ACTIVE'`, customerID).Scan(&active)
	if err != nil {
		return fmt.Errorf("count active campaigns: %w", err)
	}
	if s.cfg.SelfServeMaxActiveCampaigns > 0 && int(active) >= s.cfg.SelfServeMaxActiveCampaigns {
		return ErrSelfServeActiveCampaignLimit
	}

	startOfDay := time.Now().UTC().Truncate(24 * time.Hour)
	var createdToday int64
	err = s.GetPool().QueryRow(ctx, `
		SELECT COUNT(*) FROM campaigns
		WHERE customer_id = $1 AND created_at >= $2`, customerID, startOfDay).Scan(&createdToday)
	if err != nil {
		return fmt.Errorf("count daily campaign creates: %w", err)
	}
	if s.cfg.SelfServeMaxCreatesPerDay > 0 && int(createdToday) >= s.cfg.SelfServeMaxCreatesPerDay {
		return ErrSelfServeDailyCreateLimit
	}
	return nil
}
