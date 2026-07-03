package templates

import "strings"

const deploymentDiagram = `nginx:8180
  → tracker-0..3:8181-8184 (stateless ingest + bid)
  → processor:8186 (stream consumer → CH)
management:8188 → postgres:5430
auth:51051 (gRPC)
redis-0..5:6479-6484 (sharded)
clickhouse:9000/:8123 HTTP`

type UserDTO struct {
	ID          string   `json:"id"`
	Email       string   `json:"email,omitempty"`
	Role        string   `json:"role"`
	CustomerID  string   `json:"customer_id"`
	Permissions []string `json:"permissions,omitempty"`
}

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
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
}

func RoleLabel(role string) string {
	switch strings.ToUpper(strings.TrimSpace(role)) {
	case "SA", "SUPERADMIN", "ADMIN":
		return "Super Admin"
	case "M", "MANAGER":
		return "Manager"
	case "C", "CUSTOMER", "USER":
		return "Customer"
	case "G", "GUEST":
		return "Guest"
	default:
		return role
	}
}
