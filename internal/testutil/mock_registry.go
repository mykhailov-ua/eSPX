package testutil

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/domain"

	"github.com/google/uuid"
)

// MockCampaignRegistry is an in-memory CampaignRegistry for handler and filter tests.
type MockCampaignRegistry struct{}

func (m *MockCampaignRegistry) Exists(id uuid.UUID) bool { return true }

func (m *MockCampaignRegistry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode domain.PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
}

func (m *MockCampaignRegistry) GetCustomerID(id uuid.UUID) (uuid.UUID, bool) { return uuid.Nil, true }

var (
	mockCampaignTemplateMu sync.RWMutex
	mockCampaignTemplate   = &domain.Campaign{CustomerID: uuid.Nil, Location: time.UTC}
	mockCampaignCache      atomic.Pointer[domain.Campaign]
)

func (m *MockCampaignRegistry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	if got := mockCampaignCache.Load(); got != nil && got.ID == id {
		return got, true
	}

	mockCampaignTemplateMu.RLock()
	defer mockCampaignTemplateMu.RUnlock()

	cp := *mockCampaignTemplate
	cp.ID = id
	idStr := id.String()
	custStr := cp.CustomerID.String()
	cp.IDStr = idStr
	cp.IDStrAny = idStr
	cp.CustomerIDStr = custStr
	cp.CustomerIDStrAny = custStr

	cp.BudgetCampaignKey = "budget:campaign:" + idStr
	cp.CampaignSyncKey = "budget:sync:campaign:" + idStr
	cp.CustomerSyncKey = "budget:sync:customer:" + custStr
	if cp.BrandFcapKey != "" {
		cp.FcapKeyPrefix = cp.BrandFcapKey + ":u:"
	} else {
		cp.FcapKeyPrefix = "fcap:c:" + idStr + ":u:"
	}
	cp.DailySpendKeyPrefix = "budget:daily_spent:campaign:" + idStr + ":"

	mockCampaignCache.Store(&cp)
	return &cp, true
}

func (m *MockCampaignRegistry) Sync(ctx context.Context) (int, error) { return 0, nil }

func (m *MockCampaignRegistry) StartSync(ctx context.Context, interval time.Duration) {}

func (m *MockCampaignRegistry) Wait(ctx context.Context) error { return nil }

func SetMockCampaign(t testing.TB, c *domain.Campaign) {
	t.Helper()
	mockCampaignCache.Store(c)
	t.Cleanup(func() { mockCampaignCache.Store(nil) })
}

func StoreMockCampaign(c *domain.Campaign) {
	mockCampaignCache.Store(c)
}

func ClearMockCampaign() {
	mockCampaignCache.Store(nil)
}

func PatchMockCampaignTemplate(fn func(*domain.Campaign)) {
	mockCampaignTemplateMu.Lock()
	defer mockCampaignTemplateMu.Unlock()
	fn(mockCampaignTemplate)
}
