package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// CampaignStatus is the shared lifecycle vocabulary for management, workers, and hot-path delivery checks.
type CampaignStatus string

const (
	CampaignStatusActive    CampaignStatus = "ACTIVE"
	CampaignStatusPaused    CampaignStatus = "PAUSED"
	CampaignStatusExhausted CampaignStatus = "EXHAUSTED"
)

// PacingMode is the delivery throttle mode propagated to Redis and the filter engine.
type PacingMode string

const (
	PacingModeAsap PacingMode = "ASAP"
	PacingModeEven PacingMode = "EVEN"
)

// Campaign is the hot-path campaign view with precomputed Redis keys and bound fields for allocation-free filter evaluation.
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

	StartAt      *time.Time
	EndAt        *time.Time
	DaypartHours map[int16]struct{}

	// Fraud tier upper bounds (0-100); block tier is scores > FraudThresholdIVT up to 100.
	FraudThresholdPass    uint8
	FraudThresholdSuspect uint8
	FraudThresholdIVT     uint8
	FraudThresholdBlock   uint8
	GhostIVTEnabled       bool
	BehaviorFlags         BehaviorFlags
}

// BehaviorFlags enables per-campaign behavioral filter checks on the ingest hot path.
type BehaviorFlags uint32

const (
	BehaviorRequireImp BehaviorFlags = 1 << iota // beh_no_imp: reject click without imp_ts
	BehaviorLowTTC                               // beh_low_ttc: sub-threshold time-to-click
	BehaviorVelIP                                // beh_vel_ip: IP velocity window
	BehaviorVelUser                              // beh_vel_user: user-campaign click velocity
	BehaviorConvFast                             // beh_conv_fast: conversion within 5s of click (Go)
	BehaviorSeqGap                               // beh_seq_gap: skipped funnel step
	BehaviorDwellProxy                           // beh_dwell_prx: click before render dwell minimum
)

// DefaultFraudThresholds matches PLAN.md tier boundaries until campaign overrides are set.
const (
	DefaultFraudThresholdPass    uint8 = 30
	DefaultFraudThresholdSuspect uint8 = 60
	DefaultFraudThresholdIVT     uint8 = 80
	DefaultFraudThresholdBlock   uint8 = 100
)

// Brand groups creatives and frequency caps under one advertiser for cross-campaign enforcement.
type Brand struct {
	ID         uuid.UUID
	CustomerID uuid.UUID
	Name       string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

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
