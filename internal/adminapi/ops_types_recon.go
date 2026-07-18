package adminapi

// ReconRunDTO is a unified recon run record from management or payment services.
type ReconRunDTO struct {
	Service            string `json:"service"`
	ID                 int64  `json:"id"`
	PeriodStart        string `json:"period_start"`
	PeriodEnd          string `json:"period_end"`
	Status             string `json:"status"`
	TotalDelta         *int64 `json:"total_delta,omitempty"`
	CampaignsChecked   *int32 `json:"campaigns_checked,omitempty"`
	DiscrepanciesFound *int32 `json:"discrepancies_found,omitempty"`
	FindingsCount      *int32 `json:"findings_count,omitempty"`
	IntentsChecked     *int32 `json:"intents_checked,omitempty"`
	ErrorMessage       string `json:"error_message,omitempty"`
	CreatedAt          string `json:"created_at"`
	CompletedAt        string `json:"completed_at,omitempty"`
}
