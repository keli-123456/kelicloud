package api

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/models"
)

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
	return accounts.IsUserRoleAtLeast(user.Role, accounts.RoleAdmin), nil
}
