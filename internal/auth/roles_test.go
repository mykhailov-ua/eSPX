package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeRole(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"admin", RoleAdmin},
		{"A", RoleAdmin},
		{"manager", RoleManager},
		{"U", RoleUser},
		{"customer", RoleUser},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, NormalizeRole(tc.in), tc.in)
	}
}

func TestValidateRegisterRole(t *testing.T) {
	role, err := ValidateRegisterRole("")
	assert.NoError(t, err)
	assert.Equal(t, RoleUser, role)

	role, err = ValidateRegisterRole("manager")
	assert.NoError(t, err)
	assert.Equal(t, RoleManager, role)

	_, err = ValidateRegisterRole("invalid")
	assert.ErrorIs(t, err, ErrValidation)
}
