package api

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/clients"
)

func GetClientRecentRecords(c *gin.Context) {
	uuid := c.Param("uuid")

	if uuid == "" {
		RespondError(c, 400, "UUID is required")
		return
	}

	userUUID, ok := RequireUserScopeFromSession(c)
	if !ok {
		return
	}

	if _, err := clients.GetClientByUUIDForUser(uuid, userUUID); err != nil {
		RespondError(c, 404, "Client not found")
		return
	}
	records, _ := Records.Get(uuid)
	RespondSuccess(c, records)
}
