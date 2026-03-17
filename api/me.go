package api

import (
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/accounts"

	"github.com/gin-gonic/gin"
)

func GetMe(c *gin.Context) {
	session, err := c.Cookie("session_token")
	if err != nil {
		c.JSON(200, gin.H{"username": "Guest", "logged_in": false})
		return
	}
	sessionRecord, err := accounts.GetSessionRecord(session)
	if err != nil {
		c.JSON(200, gin.H{"username": "Guest", "logged_in": false})
		return
	}
	uuid := sessionRecord.UUID
	user, err := accounts.GetUserByUUID(uuid)
	if err != nil {
		c.JSON(200, gin.H{"username": "Guest", "logged_in": false})
		return
	}
	policy, err := config.GetUserPolicy(user.UUID)
	if err != nil {
		policy = config.UserPolicy{}
	}

	c.JSON(200, gin.H{
		"username":           user.Username,
		"logged_in":          true,
		"uuid":               user.UUID,
		"role":               user.Role,
		"sso_type":           user.SSOType,
		"sso_id":             user.SSOID,
		"2fa_enabled":        user.TwoFactor != "",
		"server_quota":       policy.ServerQuota,
		"allowed_features":   policy.AllowedFeatures,
		"available_features": config.UserAvailableFeatures(),
	})

}
