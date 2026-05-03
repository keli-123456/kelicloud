package api

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/models"
)

func userAccessDeniedMessage(status string) string {
	switch status {
	case config.UserAccessStatusDisabled:
		return "User account is disabled"
	case config.UserAccessStatusExpired:
		return "User account has expired"
	default:
		return "User account is not active"
	}
}

func validateUserAccess(userUUID string) (bool, string, error) {
	return config.IsUserAccessActive(userUUID, time.Now())
}

func rejectInactiveUser(c *gin.Context, userUUID string) bool {
	active, status, err := validateUserAccess(userUUID)
	if err != nil {
		RespondError(c, 500, "Failed to load user access policy: "+err.Error())
		return true
	}
	if !active {
		RespondError(c, 403, userAccessDeniedMessage(status))
		return true
	}
	return false
}

func ResolveSessionUser(c *gin.Context) (models.User, bool, error) {
	session, cookieErr := c.Cookie("session_token")
	if cookieErr != nil || strings.TrimSpace(session) == "" {
		return models.User{}, false, nil
	}

	sessionRecord, err := accounts.GetSessionRecord(session)
	if err != nil {
		return models.User{}, false, err
	}

	user, err := accounts.GetUserByUUID(sessionRecord.UUID)
	if err != nil {
		return models.User{}, false, err
	}
	return user, true, nil
}

func RequireSessionUser(c *gin.Context) (models.User, bool) {
	if c == nil {
		return models.User{}, false
	}

	user, loggedIn, err := ResolveSessionUser(c)
	if err != nil {
		RespondError(c, 500, "Failed to resolve session user: "+err.Error())
		return models.User{}, false
	}
	if !loggedIn || strings.TrimSpace(user.UUID) == "" {
		RespondError(c, 401, "Login required")
		return models.User{}, false
	}
	if rejectInactiveUser(c, user.UUID) {
		return models.User{}, false
	}
	return user, true
}

func IsSessionUserPlatformAdmin(c *gin.Context) (bool, error) {
	user, loggedIn, err := ResolveSessionUser(c)
	if err != nil {
		return false, err
	}
	if !loggedIn {
		return false, nil
	}
	active, _, err := validateUserAccess(user.UUID)
	if err != nil || !active {
		return false, err
	}
	return accounts.IsUserRoleAtLeast(user.Role, accounts.RoleAdmin), nil
}
