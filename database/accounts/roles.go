package accounts

import "strings"

const (
	RoleAdmin = "admin"
	RoleUser  = "user"
)

func NormalizeUserRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case RoleUser:
		return RoleUser
	case RoleAdmin:
		return RoleAdmin
	default:
		// Legacy users had no explicit global role and effectively behaved as admins.
		return RoleAdmin
	}
}

func ParseUserRole(role string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case RoleAdmin:
		return RoleAdmin, true
	case RoleUser:
		return RoleUser, true
	default:
		return "", false
	}
}

func IsUserRoleAtLeast(role, expected string) bool {
	return userRoleRank(role) >= userRoleRank(expected)
}

func userRoleRank(role string) int {
	switch NormalizeUserRole(role) {
	case RoleAdmin:
		return 2
	case RoleUser:
		return 1
	default:
		return 0
	}
}
