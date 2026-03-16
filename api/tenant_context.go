package api

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/accounts"
)

func ResolveTenantScopeFromSession(c *gin.Context) (tenantID string, loggedIn bool, err error) {
	session, cookieErr := c.Cookie("session_token")
	if cookieErr == nil && session != "" {
		return accounts.ResolveTenantScope(session)
	}

	for _, candidate := range []string{
		strings.TrimSpace(c.GetHeader("X-Komari-Tenant")),
		strings.TrimSpace(c.Query("tenant")),
	} {
		if candidate == "" {
			continue
		}
		tenant, err := database.GetTenantByIdentifier(candidate)
		if err == nil {
			return tenant.ID, false, nil
		}
	}

	return accounts.ResolveTenantScope("")
}
