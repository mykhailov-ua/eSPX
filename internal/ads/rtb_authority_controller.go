package ads

import "espx/internal/config"

// RtbAuthorityController applies RTB budget authority from system_settings to hot-path components.
type RtbAuthorityController struct {
	cfg        *config.Config
	watcher    *SettingsWatcher
	unified    *UnifiedFilter
	catalog    *RtbCatalog
	budgetSync *RtbBudgetSync
}

// NewRtbAuthorityController wires dynamic RTB_BUDGET_AUTHORITY propagation.
func NewRtbAuthorityController(
	cfg *config.Config,
	watcher *SettingsWatcher,
	unified *UnifiedFilter,
	catalog *RtbCatalog,
	budgetSync *RtbBudgetSync,
) *RtbAuthorityController {
	c := &RtbAuthorityController{
		cfg:        cfg,
		watcher:    watcher,
		unified:    unified,
		catalog:    catalog,
		budgetSync: budgetSync,
	}
	if watcher != nil {
		watcher.AddChangeListener(func(_ *DynamicConfig) { c.Apply() })
	}
	c.Apply()
	return c
}

// Apply refreshes Lua skip-budget and in-process RTB authority from the latest settings snapshot.
func (c *RtbAuthorityController) Apply() {
	setting := ""
	if c.watcher != nil {
		setting = c.watcher.Get().RtbBudgetAuthority
	}
	auth := BudgetAuthorityFromSettings(c.cfg, setting)
	if c.unified != nil {
		c.unified.SetSkipBudgetDebit(RtbSkipLuaBudgetDebit(c.cfg, setting))
	}
	if c.catalog != nil {
		c.catalog.SetAuthority(auth)
	}
	if c.budgetSync != nil {
		c.budgetSync.Authority = auth
	}
}
