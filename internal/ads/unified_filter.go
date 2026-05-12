package ads

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/redis/go-redis/v9"
)

//go:embed unified_filter.lua
var unifiedFilterLua string

type UnifiedFilter struct {
	rdbs             []redis.UniversalClient
	script           *redis.Script
	registry         domain.CampaignRegistry
	repo             domain.CampaignRepository
	rateLimit        int
	rateLimitWindow  time.Duration
	dupTTL           time.Duration
	idempotencyTTL   time.Duration
	clickAmount      float64
	impressionAmount float64
	streamName       string
	maxStreamLen     int
}

func NewUnifiedFilter(
	rdbs []redis.UniversalClient,
	registry domain.CampaignRegistry,
	repo domain.CampaignRepository,
	rateLimit int,
	rateLimitWindow time.Duration,
	dupTTL time.Duration,
	idempotencyTTL time.Duration,
	clickAmount float64,
	impressionAmount float64,
	streamName string,
	maxStreamLen int,
) *UnifiedFilter {
	return &UnifiedFilter{
		rdbs:             rdbs,
		script:           redis.NewScript(unifiedFilterLua),
		registry:         registry,
		repo:             repo,
		rateLimit:        rateLimit,
		rateLimitWindow:  rateLimitWindow,
		dupTTL:           dupTTL,
		idempotencyTTL:   idempotencyTTL,
		clickAmount:      clickAmount,
		impressionAmount: impressionAmount,
		streamName:       streamName,
		maxStreamLen:     maxStreamLen,
	}
}

func (f *UnifiedFilter) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(f.rdbs) == 1 {
		return f.rdbs[0]
	}
	// Implements a deterministic summation-based hash to distribute campaigns across Redis shards.
	sum := 0
	for _, b := range campaignID {
		sum += int(b)
	}
	return f.rdbs[sum%len(f.rdbs)]
}

func (f *UnifiedFilter) Check(ctx context.Context, evt *domain.Event) error {
	rdb := f.getRDB(evt.CampaignID)
	customerID, ok := f.registry.GetCustomerID(evt.CampaignID)
	if !ok {
		return fmt.Errorf("campaign not found in registry: %s", evt.CampaignID)
	}

	if evt.ClickID == "" {
		id, _ := uuid.NewV7()
		evt.ClickID = id.String()
	}

	rlKey := fmt.Sprintf("rl:ip:%s", evt.IP)

	dupKey := fmt.Sprintf("dup:%s:%s", evt.Type, evt.ClickID)
	budgetSourceKey := fmt.Sprintf("budget:campaign:%s", evt.CampaignID)
	idempotencyKey := fmt.Sprintf("idempotency:click:%s", evt.ClickID)
	campaignSyncKey := fmt.Sprintf("budget:sync:campaign:%s", evt.CampaignID)
	customerSyncKey := fmt.Sprintf("budget:sync:customer:%s", customerID)
	dirtyCampaignsKey := "budget:dirty_campaigns"
	dirtyCustomersKey := "budget:dirty_customers"
	streamKey := f.streamName

	amount := f.clickAmount
	if evt.Type == "impression" {
		amount = f.impressionAmount
	}

	res, err := f.script.Run(ctx, rdb,
		[]string{
			rlKey,
			dupKey,
			budgetSourceKey,
			idempotencyKey,
			campaignSyncKey,
			customerSyncKey,
			dirtyCampaignsKey,
			dirtyCustomersKey,
			streamKey,
		},
		int(f.rateLimitWindow.Seconds()),
		f.rateLimit,
		int(f.dupTTL.Seconds()),
		amount,
		int(f.idempotencyTTL.Seconds()),
		evt.CampaignID.String(),
		customerID.String(),
		f.maxStreamLen,
		evt.ClickID,
		evt.Type,
		string(evt.Payload),
		evt.IP,
		evt.UA,
	).Int64()

	if err != nil {
		return nil // Implements a fail-open policy to maintain ingestion availability during transient infrastructure failures.
	}

	if res == -1 {
		// Fetches budget data from PostgreSQL when the Redis cache is cold and re-triggers the validation script.
		camp, err := f.repo.GetByID(ctx, evt.CampaignID)
		if err != nil {
			return nil // Implements a fail-open policy to maintain ingestion availability during transient infrastructure failures.
		}

		remaining := camp.BudgetLimit - camp.CurrentSpend
		if remaining < 0 {
			remaining = 0
		}

		// Seeds the Redis budget cache with a 24-hour expiration to prevent redundant database lookups.
		rdb.SetNX(ctx, budgetSourceKey, remaining, 24*time.Hour)

		// Re-executes the filter logic after a cache seed to complete the validation cycle.
		return f.Check(ctx, evt)
	}

	switch res {
	case 1:
		return ErrRateLimitExceeded
	case 2:
		return ErrDuplicateEvent
	case 3:
		return ErrBudgetExhausted
	default:
		return nil
	}
}
