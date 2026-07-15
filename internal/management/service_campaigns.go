package management

import (
	"context"
	"fmt"
	"time"

	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"
	"espx/pkg/coldpath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CampaignDTO exposes campaign state and delivery settings to the admin API.
type CampaignDTO struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Status          string   `json:"status"`
	BudgetLimit     string   `json:"budget_limit"`
	CurrentSpend    string   `json:"current_spend"`
	CustomerID      string   `json:"customer_id"`
	PacingMode      string   `json:"pacing_mode"`
	DailyBudget     string   `json:"daily_budget"`
	Timezone        string   `json:"timezone"`
	FreqLimit       int32    `json:"freq_limit"`
	FreqWindow      int32    `json:"freq_window"`
	TargetCountries []string `json:"target_countries"`
	StartAt         string   `json:"start_at,omitempty"`
	EndAt           string   `json:"end_at,omitempty"`
	DaypartHours    []int16  `json:"daypart_hours"`
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
}

// StatusHistoryDTO records a campaign status transition for audit and troubleshooting.
type StatusHistoryDTO struct {
	ID         int64  `json:"id"`
	CampaignID string `json:"campaign_id"`
	OldStatus  string `json:"old_status,omitempty"`
	NewStatus  string `json:"new_status"`
	Reason     string `json:"reason,omitempty"`
	CreatedAt  string `json:"created_at"`
}

func statusHistoryToDTO(r db.CampaignStatusHistory) StatusHistoryDTO {
	var oldStatus string
	if r.OldStatus.Valid {
		oldStatus = string(r.OldStatus.CampaignStatusType)
	}
	return StatusHistoryDTO{
		ID:         r.ID,
		CampaignID: uuid.UUID(r.CampaignID.Bytes).String(),
		OldStatus:  oldStatus,
		NewStatus:  string(r.NewStatus),
		Reason:     r.Reason.String,
		CreatedAt:  r.CreatedAt.Time.Format(time.RFC3339),
	}
}

// toCampaignDTO maps a database campaign row into the admin API representation.
func toCampaignDTO(c db.Campaign) CampaignDTO {
	countries := c.TargetCountries
	if countries == nil {
		countries = []string{}
	}

	return CampaignDTO{
		ID:              uuid.UUID(c.ID.Bytes).String(),
		Name:            c.Name,
		Status:          string(c.Status),
		BudgetLimit:     formatMicro(c.BudgetLimit),
		CurrentSpend:    formatMicro(c.CurrentSpend),
		CustomerID:      uuid.UUID(c.CustomerID.Bytes).String(),
		PacingMode:      string(c.PacingMode),
		DailyBudget:     formatMicro(c.DailyBudget),
		Timezone:        c.Timezone,
		FreqLimit:       c.FreqLimit.Int32,
		FreqWindow:      c.FreqWindow.Int32,
		TargetCountries: countries,
		StartAt:         formatOptionalTime(c.StartAt),
		EndAt:           formatOptionalTime(c.EndAt),
		DaypartHours:    daypartOrEmpty(c.DaypartHours),
		CreatedAt:       c.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:       c.UpdatedAt.Time.Format(time.RFC3339),
	}
}

// formatOptionalTime renders optional schedule timestamps for JSON responses.
func formatOptionalTime(t pgtype.Timestamptz) string {
	if !t.Valid {
		return ""
	}
	return t.Time.Format(time.RFC3339)
}

// daypartOrEmpty normalizes nil daypart slices to empty JSON arrays.
func daypartOrEmpty(h []int16) []int16 {
	if h == nil {
		return []int16{}
	}
	return h
}

// ListCampaigns returns paginated campaigns filtered by customer and status for the admin UI.
func (s *Service) ListCampaigns(ctx context.Context, customerID uuid.UUID, status string, limit, offset int32) ([]CampaignDTO, int64, error) {
	q := db.New(s.GetPool())

	var cid pgtype.UUID
	if customerID != uuid.Nil {
		cid = ingestion.ToUUID(customerID)
	}

	var st pgtype.Text
	if status != "" {
		st = pgtype.Text{String: status, Valid: true}
	}

	countParams := db.CountCampaignsParams{
		CustomerID: cid,
		Status:     st,
	}
	listParams := db.ListCampaignsParams{
		Limit:      limit,
		Offset:     offset,
		CustomerID: cid,
		Status:     st,
	}

	return coldpath.PaginatedList(
		func() (int64, error) { return q.CountCampaigns(ctx, countParams) },
		func() ([]db.Campaign, error) { return q.ListCampaigns(ctx, listParams) },
		toCampaignDTO,
	)
}

// GetCampaignDTO loads a single campaign for detail views and access checks.
func (s *Service) GetCampaignDTO(ctx context.Context, id uuid.UUID) (CampaignDTO, error) {
	q := db.New(s.GetPool())
	c, err := q.GetCampaignFull(ctx, ingestion.ToUUID(id))
	if err != nil {
		return CampaignDTO{}, mapNotFound(err, ErrCampaignNotFound)
	}
	return toCampaignDTO(c), nil
}

// ListStatusHistory returns paginated status transitions for a campaign audit trail.
func (s *Service) ListStatusHistory(ctx context.Context, campaignID uuid.UUID, limit, offset int32) ([]StatusHistoryDTO, int64, error) {
	q := db.New(s.GetPool())
	cid := ingestion.ToUUID(campaignID)

	listParams := db.ListStatusHistoryParams{
		CampaignID: cid,
		Limit:      limit,
		Offset:     offset,
	}
	return coldpath.PaginatedList(
		func() (int64, error) { return q.CountStatusHistory(ctx, cid) },
		func() ([]db.CampaignStatusHistory, error) { return q.ListStatusHistory(ctx, listParams) },
		statusHistoryToDTO,
	)
}

// UpdateCampaignPacing changes manual pacing mode and propagates the update to the hot path via coldpath.
func (s *Service) UpdateCampaignPacing(ctx context.Context, campaignID uuid.UUID, newMode string) (CampaignDTO, error) {
	var pacing db.PacingModeType
	switch newMode {
	case "ASAP":
		pacing = db.PacingModeTypeASAP
	case "EVEN":
		pacing = db.PacingModeTypeEVEN
	default:
		return CampaignDTO{}, fmt.Errorf("%w: %s", ErrInvalidPacingMode, newMode)
	}

	var updatedCamp db.Campaign
	err := pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)

		camp, err := q.GetCampaignForUpdate(ctx, ingestion.ToUUID(campaignID))
		if err != nil {
			return mapNotFound(err, ErrCampaignNotFound)
		}

		updatedCamp, err = q.UpdateCampaignPacing(ctx, db.UpdateCampaignPacingParams{
			ID:         ingestion.ToUUID(campaignID),
			PacingMode: pacing,
		})
		if err != nil {
			return fmt.Errorf("failed to update campaign pacing: %w", err)
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}

		s.AuditLog(ctx, q, uid, "UPDATE_CAMPAIGN_PACING", "campaign", &campaignID, map[string]any{
			"old_pacing_mode": string(camp.PacingMode),
			"new_pacing_mode": string(pacing),
		}, nil)

		payloadBytes, err := coldpath.MarshalJSON(map[string]any{
			"campaign_id": campaignID.String(),
			"pacing_mode": string(pacing),
		})
		if err != nil {
			return fmt.Errorf("marshal update campaign pacing outbox payload: %w", err)
		}

		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "UPDATE_CAMPAIGN_PACING",
			Payload:   payloadBytes,
		})
		if err != nil {
			return fmt.Errorf("failed to create outbox event: %w", err)
		}

		return nil
	})

	if err != nil {
		return CampaignDTO{}, err
	}

	return toCampaignDTO(updatedCamp), nil
}
