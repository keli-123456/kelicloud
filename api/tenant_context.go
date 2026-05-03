package api

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/accounts"
)

func ResolveUserScopeFromSession(c *gin.Context) (userUUID string, loggedIn bool, err error) {
	session, cookieErr := c.Cookie("session_token")
	if cookieErr != nil || strings.TrimSpace(session) == "" {
		return "", false, nil
	}

	sessionRecord, err := accounts.GetSessionRecord(session)
	if err != nil {
		return "", false, err
	}

	return sessionRecord.UUID, true, nil
}

func RequireUserScopeFromSession(c *gin.Context) (userUUID string, ok bool) {
	if c == nil {
		return "", false
	}

	userUUID, loggedIn, err := ResolveUserScopeFromSession(c)
	if err != nil {
		RespondError(c, 500, "Failed to resolve user scope: "+err.Error())
		return "", false
	}
	userUUID = strings.TrimSpace(userUUID)
	if !loggedIn || userUUID == "" {
		RespondError(c, 401, "Login required")
		return "", false
	}
	if rejectInactiveUser(c, userUUID) {
		return "", false
	}
	return userUUID, true
}
