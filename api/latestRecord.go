package api

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/clients"
)

func GetClientRecentRecords(c *gin.Context) {
	uuid := c.Param("uuid")

	if uuid == "" {
		RespondError(c, 400, "UUID is required")
		return
	}

	user, ok := RequireSessionUser(c)
	if !ok {
		return
	}

	isAdmin := accounts.IsUserRoleAtLeast(user.Role, accounts.RoleAdmin)
	if !isAdmin {
		if _, err := clients.GetClientByUUIDForUser(uuid, user.UUID); err != nil {
			RespondError(c, 404, "Client not found")
			return
		}
	} else if _, err := clients.GetClientByUUID(uuid); err != nil {
		RespondError(c, 404, "Client not found")
		return
	}
	records, _ := Records.Get(uuid)
	RespondSuccess(c, records)
}
