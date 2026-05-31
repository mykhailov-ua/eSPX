// Package domain contains the core business entities and repository interfaces
// for the ad-event pipeline. Types in this package are shared across the ads,
// management, and auth packages; they carry no framework dependencies.
//
// Campaign pre-computes several string and any-boxed fields (IDStr, IDStrAny,
// BrandFcapKey, BudgetCampaignKey, etc.) at construction time so that the
// filter hot path can reference them without repeated formatting. The any-typed
// fields (DailyBudgetMicroAny, FreqLimitAny, FreqWindowAny) are pre-boxed to
// avoid per-call interface boxing inside the Lua ARGV assembly path.
package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type CampaignStatus string

const (
	CampaignStatusActive    CampaignStatus = "ACTIVE"
	CampaignStatusPaused    CampaignStatus = "PAUSED"
	CampaignStatusExhausted CampaignStatus = "EXHAUSTED"
)

type PacingMode string

const (
	PacingModeAsap PacingMode = "ASAP"
	PacingModeEven PacingMode = "EVEN"
)

// Campaign holds the runtime state of a single campaign cached in the in-process
// registry. Fields are grouped by use: UUID identifiers, pre-formatted string
// variants for zero-alloc key assembly, budget/pacing parameters, and targeting
// constraints. Callers must treat the struct as immutable after insertion into
// the registry map.
type Campaign struct {
	ID                  uuid.UUID
	CustomerID          uuid.UUID
	IDStr               string
	CustomerIDStr       string
	IDStrAny            any
	CustomerIDStrAny    any
	BrandFcapKey        string
	Name                string
	Status              CampaignStatus
	PacingMode          PacingMode
	DailyBudgetMicroAny any
	Timezone            string
	FreqLimitAny        any
	FreqWindowAny       any
	BudgetCampaignKey   string
	CampaignSyncKey     string
	CustomerSyncKey     string
	FcapKeyPrefix       string
	DailySpendKeyPrefix string

	BrandID          *uuid.UUID
	BudgetLimit      int64
	CurrentSpend     int64
	DailyBudget      int64
	DailyBudgetMicro int64
	Location         *time.Location
	TargetCountries  map[string]struct{}

	FreqLimit  int32
	FreqWindow int32
}

type Brand struct {
	ID         uuid.UUID
	CustomerID uuid.UUID
	Name       string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// CampaignRepository defines the persistence interface for campaign lifecycle
// operations. UpdateSpend must be idempotent with respect to the txID argument;
// the implementation uses INSERT INTO sync_idempotency ON CONFLICT DO NOTHING.
type CampaignRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Campaign, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status CampaignStatus) error
	UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error
	ListActive(ctx context.Context) ([]*Campaign, error)
}

// CampaignRegistry defines the in-process cache interface used by filter components.
// Add inserts a campaign with its full targeting metadata; Sync refreshes the cache
// from the database. All methods must be safe for concurrent use.
type CampaignRegistry interface {
	Exists(id uuid.UUID) bool
	Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string)
	GetCustomerID(id uuid.UUID) (uuid.UUID, bool)
	GetCampaign(id uuid.UUID) (*Campaign, bool)
	Sync(ctx context.Context) (int, error)
	StartSync(ctx context.Context, interval time.Duration)
	Wait(ctx context.Context) error
}
