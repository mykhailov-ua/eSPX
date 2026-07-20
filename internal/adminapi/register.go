package adminapi

import "net/http"

// RouteRegistry wires cold-path admin JSON handlers into cmd/management.
type RouteRegistry struct {
	BillingHTTP    *BillingHTTPHandlers
	OpsHTTP        *OpsHTTPHandlers
	ExportHTTP     *ExportHTTPHandlers
	LicensingHTTP  *LicensingHTTPHandlers
	ReportsHTTP    *ReportsHTTPHandlers
	DashboardsHTTP *DashboardsHTTPHandlers
	ViewsHTTP      *ViewsHTTPHandlers
	SelfServeHTTP  *SelfServeHTTPHandlers
	PostbackHTTP    *PostbackHTTPHandlers
	CostSyncHTTP    *CostSyncHTTPHandlers
	MarginGuardHTTP *MarginGuardHTTPHandlers
}

// RegisterRoutes mounts adminapi handlers on mux.
func RegisterRoutes(mux *http.ServeMux, routes RouteRegistry) {
	if routes.BillingHTTP != nil {
		routes.BillingHTTP.Register(mux)
	}
	if routes.OpsHTTP != nil {
		routes.OpsHTTP.Register(mux)
	}
	if routes.ExportHTTP != nil {
		routes.ExportHTTP.Register(mux)
	}
	if routes.LicensingHTTP != nil {
		routes.LicensingHTTP.Register(mux)
	}
	if routes.ReportsHTTP != nil {
		routes.ReportsHTTP.Register(mux)
	}
	if routes.DashboardsHTTP != nil {
		routes.DashboardsHTTP.Register(mux)
	}
	if routes.ViewsHTTP != nil {
		routes.ViewsHTTP.Register(mux)
	}
	if routes.SelfServeHTTP != nil {
		routes.SelfServeHTTP.Register(mux)
	}
	if routes.PostbackHTTP != nil {
		routes.PostbackHTTP.Register(mux)
	}
	if routes.CostSyncHTTP != nil {
		routes.CostSyncHTTP.Register(mux)
	}
	if routes.MarginGuardHTTP != nil {
		routes.MarginGuardHTTP.Register(mux)
	}
}
