package api

import (
	"net/http"
	"strings"

	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/accounts"

	"github.com/gin-gonic/gin"
)

func AdminAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// API key authentication
		apiKey := c.GetHeader("Authorization")
		if isApiKeyValid(apiKey) {
			c.Set("api_key", apiKey)
			preferredTenantID := strings.TrimSpace(c.GetHeader("X-Komari-Tenant"))
			if preferredTenantID != "" {
				if tenant, err := database.GetTenantByIdentifier(preferredTenantID); err == nil {
					c.Set("tenant_id", tenant.ID)
					c.Set("tenant_role", database.RoleOwner)
				}
			}
			if _, ok := c.Get("tenant_id"); !ok {
				if tenant, err := database.GetDefaultTenant(); err == nil {
					c.Set("tenant_id", tenant.ID)
					c.Set("tenant_role", database.RoleOwner)
				}
			}
			c.Next()
			return
		}
		// session-based authentication
		session, err := c.Cookie("session_token")
		if err != nil {
			RespondError(c, http.StatusUnauthorized, "Unauthorized.")
			c.Abort()
			return
		}

		sessionRecord, err := accounts.GetSessionRecord(session)
		if err != nil {
			RespondError(c, http.StatusUnauthorized, "Unauthorized.")
			c.Abort()
			return
		}
		uuid := sessionRecord.UUID

		preferredTenantID := strings.TrimSpace(c.GetHeader("X-Komari-Tenant"))
		if preferredTenantID != "" {
			if tenant, err := database.GetTenantByIdentifier(preferredTenantID); err == nil {
				preferredTenantID = tenant.ID
			}
		}
		if preferredTenantID == "" {
			preferredTenantID = sessionRecord.CurrentTenantID
		}
		currentTenant, _, err := database.ResolveAccessibleTenant(uuid, preferredTenantID)
		if err != nil {
			RespondError(c, http.StatusForbidden, "No accessible tenant.")
			c.Abort()
			return
		}
		if currentTenant != nil && currentTenant.ID != sessionRecord.CurrentTenantID {
			_ = accounts.SetSessionCurrentTenant(session, currentTenant.ID)
		}
		accounts.UpdateLatest(session, c.Request.UserAgent(), c.ClientIP())
		// 将 session 和 用户 UUID 传递到后续处理器
		c.Set("session", session)
		c.Set("uuid", uuid)
		if currentTenant != nil {
			c.Set("tenant_id", currentTenant.ID)
			c.Set("tenant_role", currentTenant.Role)
		}

		c.Next()
	}
}
