package api

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/auditlog"
)

func AuditLogForTenant(tenantID, ip, userUUID, message, msgType string) {
	auditlog.LogForTenant(strings.TrimSpace(tenantID), ip, userUUID, message, msgType)
}

func AuditLogForCurrentTenant(c *gin.Context, userUUID, message, msgType string) {
	if c == nil {
		AuditLogForTenant("", "", userUUID, message, msgType)
		return
	}

	tenantID := ""
	if value, ok := c.Get("tenant_id"); ok {
		if text, ok := value.(string); ok {
			tenantID = text
		}
	}

	AuditLogForTenant(tenantID, c.ClientIP(), userUUID, message, msgType)
}
