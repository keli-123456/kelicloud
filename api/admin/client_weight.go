package admin

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
)

func OrderWeight(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
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
	normalizedUUIDs, err := clients.NormalizeClientUUIDsForTenant(tenantID, requestedUUIDs)
	if err != nil {
		api.RespondError(c, 400, err.Error())
		return
	}
	db := dbcore.GetDBInstance()
	for _, uuid := range normalizedUUIDs {
		err := db.Model(&models.Client{}).Where("uuid = ? AND tenant_id = ?", uuid, tenantID).Update("weight", req[uuid]).Error
		if err != nil {
			api.RespondError(c, 500, "Failed to update client weight: "+err.Error())
			return
		}
	}
	uuid, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, uuid.(string), "order clients", "info")
	api.RespondSuccess(c, nil)
}
