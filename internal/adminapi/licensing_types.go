package adminapi

import (
	"encoding/json"

	"espx/internal/licensing"
)

// SubscriptionDTO is the JSON shape for GET /api/v1/customers/{id}/subscription.
type SubscriptionDTO struct {
	CustomerID  string                  `json:"customer_id"`
	PlanCode    string                  `json:"plan_code"`
	Status      string                  `json:"status"`
	PeriodStart string                  `json:"period_start"`
	PeriodEnd   string                  `json:"period_end,omitempty"`
	Limits      licensing.LimitsDTO     `json:"limits"`
	Features    licensing.FeatureSetDTO `json:"features"`
	Effective   licensing.LimitsDTO     `json:"effective_limits"`
	Usage       []UsageMeterDTO         `json:"usage"`
}

// UsageMeterDTO is a monthly usage meter row.
type UsageMeterDTO struct {
	Meter     string `json:"meter"`
	Period    string `json:"period"`
	Value     int64  `json:"value"`
	Limit     int64  `json:"limit"`
	Remaining int64  `json:"remaining"`
}

// UsageDailyDTO is a daily usage rollup row.
type UsageDailyDTO struct {
	CustomerID string `json:"customer_id"`
	UsageDate  string `json:"usage_date"`
	Meter      string `json:"meter"`
	Value      int64  `json:"value"`
}

// QuotaStatusDTO is the RPD quota snapshot for a customer.
type QuotaStatusDTO struct {
	CustomerID string `json:"customer_id"`
	Limit      int64  `json:"limit"`
	Value      int64  `json:"value"`
	Remaining  int64  `json:"remaining"`
	Timezone   string `json:"timezone"`
}

// UpdateSubscriptionRequest is the POST body for admin subscription upsert.
type UpdateSubscriptionRequest struct {
	PlanCode      string          `json:"plan_code"`
	Status        string          `json:"status"`
	PeriodStart   string          `json:"period_start"`
	PeriodEnd     string          `json:"period_end,omitempty"`
	OverridesJSON json.RawMessage `json:"overrides_json,omitempty"`
}

// QuotaBumpRequest adds bonus daily ingress quota via overrides_json.
type QuotaBumpRequest struct {
	BonusRequests int64 `json:"bonus_requests"`
}
