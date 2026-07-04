package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// CampaignRepository isolates campaign persistence from ads and management so spend updates stay transactional.
type CampaignRepository interface {
	// GetByID loads one campaign for status and spend mutations without pulling the full active catalog.
	GetByID(ctx context.Context, id uuid.UUID) (*Campaign, error)
	// UpdateStatus propagates lifecycle changes that must reach Redis sync and delivery filters.
	UpdateStatus(ctx context.Context, id uuid.UUID, status CampaignStatus) error
	// UpdateSpend records click cost idempotently so duplicate events do not double-charge budgets.
	UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error
	// ListActive returns the delivery-eligible set that workers mirror into the in-memory registry.
	ListActive(ctx context.Context) ([]*Campaign, error)
}

// CampaignRegistry is the in-memory delivery catalog contract so handlers and sync workers share one lookup surface.
type CampaignRegistry interface {
	// Exists answers cheap eligibility checks on the ingest hot path without a store round trip.
	Exists(id uuid.UUID) bool
	// Add seeds registry state after sync or provisioning so new campaigns become bid-eligible immediately.
	Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string)
	// GetCustomerID resolves tenant ownership for spend and policy enforcement during bidding.
	GetCustomerID(id uuid.UUID) (uuid.UUID, bool)
	// GetCampaign returns the precomputed hot-path view used by filter and pacing logic.
	GetCampaign(id uuid.UUID) (*Campaign, bool)
	// Sync reloads active campaigns from persistence so delivery state tracks management changes.
	Sync(ctx context.Context) (int, error)
	// StartSync runs periodic reloads so operators need not restart ingest on every campaign edit.
	StartSync(ctx context.Context, interval time.Duration)
	// Wait blocks until an in-flight sync finishes so tests and shutdown see a consistent catalog.
	Wait(ctx context.Context) error
}
