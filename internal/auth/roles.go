package auth

import "strings"

// Role codes stored in tokens and the users table (aligned with management RBAC).
const (
	RoleAdmin   = "A"
	RoleManager = "M"
	RoleUser    = "U"
)

// NormalizeRole keeps RBAC checks stable across legacy provisioning strings and compact codes.
func NormalizeRole(role string) string {
	switch strings.ToUpper(strings.TrimSpace(role)) {
	case "SUPERADMIN", "ADMIN", "SA", "A":
		return RoleAdmin
	case "MANAGER", "M":
		return RoleManager
	case "CUSTOMER", "USER", "C", "U":
		return RoleUser
	default:
		return strings.ToUpper(strings.TrimSpace(role))
	}
}

// ValidateRegisterRole rejects unknown roles at provisioning time instead of at first login.
func ValidateRegisterRole(role string) (string, error) {
	normalized := NormalizeRole(role)
	if normalized == "" {
		return RoleUser, nil
	}
	switch normalized {
	case RoleAdmin, RoleManager, RoleUser:
		return normalized, nil
	default:
		return "", ErrValidation
	}
}
