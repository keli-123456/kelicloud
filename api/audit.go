package api

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/auditlog"
)

func AuditLogForUser(ip, userUUID, message, msgType string) {
	trimmedUserUUID := strings.TrimSpace(userUUID)
	auditlog.LogForUser(trimmedUserUUID, ip, trimmedUserUUID, message, msgType)
}

func AuditLogForCurrentUser(c *gin.Context, userUUID, message, msgType string) {
	if c == nil {
		AuditLogForUser("", userUUID, message, msgType)
		return
	}

	currentUserUUID := strings.TrimSpace(userUUID)
	if value, ok := c.Get("uuid"); ok {
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			currentUserUUID = strings.TrimSpace(text)
		}
	}

	auditlog.LogForUser(currentUserUUID, c.ClientIP(), strings.TrimSpace(userUUID), message, msgType)
}
