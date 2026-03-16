package admin

import (
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/accounts"

	"github.com/gin-gonic/gin"
)

func GetSessions(c *gin.Context) {
	uuid, _ := c.Get("uuid")

	ss, err := accounts.GetSessionsByUser(uuid.(string))
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve sessions: "+err.Error())
		return
	}
	current, _ := c.Cookie("session_token")
	c.JSON(200, gin.H{"status": "success", "current": current, "data": ss})
}

func DeleteSession(c *gin.Context) {
	var req struct {
		Session string `json:"session" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, 400, "Invalid request: "+err.Error())
		return
	}
	uuid, _ := c.Get("uuid")
	err := accounts.DeleteSessionByUser(uuid.(string), req.Session)
	if err != nil {
		api.RespondError(c, 500, "Failed to delete session: "+err.Error())
		return
	}
	api.AuditLogForCurrentTenant(c, uuid.(string), "delete session", "info")
	api.RespondSuccess(c, nil)
}

func DeleteAllSession(c *gin.Context) {
	uuid, _ := c.Get("uuid")
	err := accounts.DeleteAllSessionsByUser(uuid.(string))
	if err != nil {
		api.RespondError(c, 500, "Failed to delete all sessions: "+err.Error())
		return
	}
	api.AuditLogForCurrentTenant(c, uuid.(string), "delete all sessions", "warn")
	api.RespondSuccess(c, nil)
}
