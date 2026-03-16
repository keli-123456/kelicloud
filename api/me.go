package api

import (
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/accounts"

	"github.com/gin-gonic/gin"
)

func GetMe(c *gin.Context) {
	session, err := c.Cookie("session_token")
	if err != nil {
		c.JSON(200, gin.H{"username": "Guest", "logged_in": false, "tenants": []database.AccessibleTenant{}, "current_tenant": nil})
		return
	}
	sessionRecord, err := accounts.GetSessionRecord(session)
	if err != nil {
		c.JSON(200, gin.H{"username": "Guest", "logged_in": false, "tenants": []database.AccessibleTenant{}, "current_tenant": nil})
		return
	}
	uuid := sessionRecord.UUID
	user, err := accounts.GetUserByUUID(uuid)
	if err != nil {
		c.JSON(200, gin.H{"username": "Guest", "logged_in": false, "tenants": []database.AccessibleTenant{}, "current_tenant": nil})
		return
	}

	currentTenant, tenantList, err := database.ResolveAccessibleTenant(user.UUID, sessionRecord.CurrentTenantID)
	if err == nil && currentTenant != nil && currentTenant.ID != sessionRecord.CurrentTenantID {
		_ = accounts.SetSessionCurrentTenant(session, currentTenant.ID)
	}
	if err != nil {
		currentTenant = nil
		tenantList = []database.AccessibleTenant{}
	}

	c.JSON(200, gin.H{
		"username":       user.Username,
		"logged_in":      true,
		"uuid":           user.UUID,
		"sso_type":       user.SSOType,
		"sso_id":         user.SSOID,
		"2fa_enabled":    user.TwoFactor != "",
		"tenants":        tenantList,
		"current_tenant": currentTenant,
	})

}
