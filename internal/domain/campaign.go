package domain

import (
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

	// RequireConsentPurposes is a 16-bit mask of required consent purpose bits (M6.3).
	RequireConsentPurposes int16

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

// DefaultFraudThresholds are tier boundaries (30/60/80/100) until campaign overrides are set.
const (
	DefaultFraudThresholdPass    uint8 = 30
	DefaultFraudThresholdSuspect uint8 = 60
	DefaultFraudThresholdIVT     uint8 = 80
	DefaultFraudThresholdBlock   uint8 = 100
)
