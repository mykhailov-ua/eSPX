package repo

import (
	"time"

	"espx/internal/ads/db"
	"espx/internal/domain"

	"github.com/google/uuid"
)

// MicroUnitFactor converts dollar floats to micro-dollar integers.
const MicroUnitFactor = 1_000_000

// SliceToMap builds O(1) country lookup sets from string slices.
func SliceToMap(slice []string) map[string]struct{} {
	if slice == nil {
		return nil
	}
	m := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		m[s] = struct{}{}
	}
	return m
}

func campaignFromDBRow(row db.Campaign) *domain.Campaign {
	id := uuid.UUID(row.ID.Bytes)
	customerID := uuid.UUID(row.CustomerID.Bytes)

	loc, err := time.LoadLocation(row.Timezone)
	if err != nil {
		loc = time.UTC
	}

	var brandIDPtr *uuid.UUID
	if row.BrandID.Valid {
		brandID := uuid.UUID(row.BrandID.Bytes)
		brandIDPtr = &brandID
	}

	idStr := id.String()
	customerIDStr := customerID.String()
	dailyBudgetMicro := row.DailyBudget

	var fcapPrefix string
	if row.BrandFcapKey != "" {
		fcapPrefix = row.BrandFcapKey + ":u:"
	} else {
		fcapPrefix = "fcap:c:" + idStr + ":u:"
	}

	return &domain.Campaign{
		ID:                    id,
		IDStr:                 idStr,
		IDStrAny:              idStr,
		CustomerID:            customerID,
		CustomerIDStr:         customerIDStr,
		CustomerIDStrAny:      customerIDStr,
		BrandID:               brandIDPtr,
		BrandFcapKey:          row.BrandFcapKey,
		Name:                  row.Name,
		Status:                domain.CampaignStatus(row.Status),
		PacingMode:            domain.PacingMode(row.PacingMode),
		BudgetLimit:           row.BudgetLimit,
		CurrentSpend:          row.CurrentSpend,
		DailyBudget:           row.DailyBudget,
		DailyBudgetMicro:      dailyBudgetMicro,
		DailyBudgetMicroAny:   dailyBudgetMicro,
		Timezone:              row.Timezone,
		Location:              loc,
		FreqLimit:             row.FreqLimit.Int32,
		FreqLimitAny:          row.FreqLimit.Int32,
		FreqWindow:            row.FreqWindow.Int32,
		FreqWindowAny:         row.FreqWindow.Int32,
		TargetCountries:       SliceToMap(row.TargetCountries),
		BudgetCampaignKey:     "budget:campaign:" + idStr,
		CampaignSyncKey:       "budget:sync:campaign:" + idStr,
		CustomerSyncKey:       "budget:sync:customer:" + customerIDStr,
		FcapKeyPrefix:         fcapPrefix,
		DailySpendKeyPrefix:   "budget:daily_spent:campaign:" + idStr + ":",
		FraudThresholdPass:    uint8(row.FraudThresholdPass),
		FraudThresholdSuspect: uint8(row.FraudThresholdSuspect),
		FraudThresholdIVT:     uint8(row.FraudThresholdIvt),
		FraudThresholdBlock:   uint8(row.FraudThresholdBlock),
		GhostIVTEnabled:       row.GhostIvtEnabled,
		BehaviorFlags:         domain.BehaviorFlags(row.BehaviorFlags),
	}
}

// CampaignFromDBRow maps a sqlc campaign row to a domain campaign.
func CampaignFromDBRow(row db.Campaign) *domain.Campaign {
	return campaignFromDBRow(row)
}
