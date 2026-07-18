package adminapi

import (
	"context"

	"github.com/google/uuid"
	"time"
)

// CreateCampaignInput is the validated self-serve campaign creation payload.
type CreateCampaignInput struct {
	CustomerID       uuid.UUID
	BrandID          *uuid.UUID
	Name             string
	BudgetLimitMicro int64
	PacingMode       string
	DailyBudgetMicro int64
	Timezone         string
	FreqLimit        int32
	FreqWindow       int32
	TargetCountries  []string
	StartAt          *time.Time
	EndAt            *time.Time
	DaypartHours     []int16
	IdempotencyKey   string
}

// CampaignAdmin creates and controls tenant-scoped campaigns.
type CampaignAdmin interface {
	EnforceSelfServeCreateLimits(ctx context.Context, customerID uuid.UUID, budgetMicro int64) error
	GenerateIdempotencyHash(customerID uuid.UUID, payload []byte) (string, error)
	CreateCampaign(ctx context.Context, spec CreateCampaignInput) (uuid.UUID, error)
	PauseCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error
	ResumeCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error
}

// PaymentIntentResult is the self-serve top-up response.
type PaymentIntentResult struct {
	IntentID    string
	Status      string
	CheckoutURL string
	ProviderRef string
}

// PaymentIntents proxies top-ups to the payment service.
type PaymentIntents interface {
	CreatePaymentIntent(ctx context.Context, customerID string, amountMicro int64, currency, idempotencyKey string, meta map[string]string) (PaymentIntentResult, error)
}

// APIKeyResult is returned when minting a machine credential.
type APIKeyResult struct {
	ID         string
	Name       string
	RawKey     string
	ExpiresAt  string
	HasExpires bool
}

// APIKeyCreator mints API keys for the authenticated session user.
type APIKeyCreator interface {
	CreateAPIKey(ctx context.Context, accessToken, name string) (APIKeyResult, error)
}

// InvoiceLister lists tenant billing history (reuses billing facet gRPC client).
type InvoiceLister = InvoiceGRPCClient
