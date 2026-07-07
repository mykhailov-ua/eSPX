package ads

import (
	"testing"

	"espx/internal/config"
	"espx/internal/rtb"
	"github.com/stretchr/testify/assert"
)

func TestRtbAuthorityController_luaKeepsBudgetInRedis(t *testing.T) {
	cfg := &config.Config{RtbMode: "live", RtbBudgetAuthority: "rtb"}
	sw := NewSettingsWatcher(nil, cfg)
	unified := &UnifiedFilter{}
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)
	sync := RtbBudgetSync{Authority: BudgetAuthorityRTB}

	ctrl := NewRtbAuthorityController(cfg, sw, unified, catalog, &sync)
	assert.True(t, unified.skipBudgetDebitAny == oneAny)
	assert.Equal(t, BudgetAuthorityRTB, catalog.Authority())

	sw.snapshot.Store(&DynamicConfig{RtbBudgetAuthority: "lua"})
	ctrl.Apply()
	assert.True(t, unified.skipBudgetDebitAny == zeroAny)
	assert.Equal(t, BudgetAuthorityRedis, catalog.Authority())
}
