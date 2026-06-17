package management

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNormalizeRole guards role alias normalization maps auth tokens to canonical RBAC roles.
func TestNormalizeRole(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{"A", RoleAdmin},
		{"admin", RoleAdmin},
		{"SA", RoleAdmin},
		{"M", RoleManager},
		{"manager", RoleManager},
		{"U", RoleUser},
		{"user", RoleUser},
		{"C", RoleUser},
		{"customer", RoleUser},
		{"unknown", "UNKNOWN"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.out, NormalizeRole(tc.in))
		})
	}
}

// TestGetPermissionsForRole guards each role receives the expected permission set.
func TestGetPermissionsForRole(t *testing.T) {
	adminPerms := []string{
		"customers:write", "customers:read",
		"campaigns:write", "campaigns:read",
		"brands:write", "brands:read",
		"settings:write", "settings:read",
		"blacklist:write", "blacklist:read",
		"audit:read", "users:write",
	}
	managerPerms := []string{
		"customers:write", "customers:read",
		"campaigns:write", "campaigns:read",
		"brands:write", "brands:read",
		"audit:read",
	}
	userPerms := []string{
		"campaigns:write", "campaigns:read",
		"customers:read",
		"brands:write", "brands:read",
	}

	tests := []struct {
		role          string
		expectedPerms []string
	}{
		{RoleAdmin, adminPerms},
		{"admin", adminPerms},
		{"SA", adminPerms},
		{RoleManager, managerPerms},
		{"manager", managerPerms},
		{RoleUser, userPerms},
		{"user", userPerms},
		{"C", userPerms},
		{"unknown", []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.role, func(t *testing.T) {
			perms := GetPermissionsForRole(tc.role)
			assert.ElementsMatch(t, tc.expectedPerms, perms)
		})
	}
}

// TestHasPermission guards permission checks respect role boundaries.
func TestHasPermission(t *testing.T) {
	assert.True(t, HasPermission(RoleAdmin, "users:write"))
	assert.False(t, HasPermission(RoleUser, "users:write"))
	assert.True(t, HasPermission(RoleUser, "brands:write"))
}
