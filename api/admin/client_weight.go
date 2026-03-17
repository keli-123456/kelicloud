package admin

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/clients"
)

func OrderWeight(c *gin.Context) {
	userUUID, ok := currentUserUUID(c)
	if !ok {
		api.RespondError(c, 403, "User context is required")
		return
	}

	var req = make(map[string]int)
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, 400, "Invalid or missing request body: "+err.Error())
		return
	}
	requestedUUIDs := make([]string, 0, len(req))
	for uuid := range req {
		requestedUUIDs = append(requestedUUIDs, uuid)
	}
	normalizedUUIDs, err := clients.NormalizeClientUUIDsForUser(userUUID, requestedUUIDs)
	if err != nil {
		api.RespondError(c, 400, err.Error())
		return
	}
	for _, uuid := range normalizedUUIDs {
		err = clients.SaveClientForUser(userUUID, map[string]interface{}{
			"uuid":   uuid,
			"weight": req[uuid],
		})
		if err != nil {
			api.RespondError(c, 500, "Failed to update client weight: "+err.Error())
			return
		}
	}
	uuid, _ := c.Get("uuid")
	api.AuditLogForCurrentUser(c, uuid.(string), "order clients", "info")
	api.RespondSuccess(c, nil)
}
