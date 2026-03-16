package admin

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/records"
	"github.com/komari-monitor/komari/ws"
	"gorm.io/gorm"
)

func currentTenantID(c *gin.Context) (string, bool) {
	tenantID, ok := c.Get("tenant_id")
	if !ok {
		return "", false
	}
	value, ok := tenantID.(string)
	return value, ok && value != ""
}

func requireCurrentTenantID(c *gin.Context) (string, bool) {
	tenantID, ok := currentTenantID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return "", false
	}
	return tenantID, true
}

func AddClient(c *gin.Context) {
	var req struct {
		Name string `json:"name"`
	}
	tenantID, ok := currentTenantID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "Tenant context is required"})
		return
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		uuid, token, err := clients.CreateClientForTenant(tenantID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "success", "uuid": uuid, "token": token})
		return
	}
	uuid, token, err := clients.CreateClientWithNameForTenant(tenantID, req.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	user_uuid, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, user_uuid.(string), "create client:"+uuid, "info")
	c.JSON(http.StatusOK, gin.H{"status": "success", "uuid": uuid, "token": token, "message": ""})
}

func EditClient(c *gin.Context) {
	var req = make(map[string]interface{})
	uuid := c.Param("uuid")
	tenantID, ok := currentTenantID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "Tenant context is required"})
		return
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	if uuid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Invalid or missing UUID"})
		return
	}
	req["uuid"] = uuid
	err := clients.SaveClientForTenant(tenantID, req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Client not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	user_uuid, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, user_uuid.(string), "edit client:"+uuid, "info")
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func RemoveClient(c *gin.Context) {
	uuid := c.Param("uuid")
	tenantID, ok := currentTenantID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "Tenant context is required"})
		return
	}
	err := clients.DeleteClientForTenant(tenantID, uuid)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"status": "error",
				"error":  "Client not found",
			})
			return
		}
		c.JSON(500, gin.H{
			"status": "error",
			"error":  "Failed to delete client" + err.Error(),
		})
		return
	}
	user_uuid, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, user_uuid.(string), "delete client:"+uuid, "warn")
	c.JSON(200, gin.H{"status": "success"})
	ws.DeleteConnectedClients(uuid)
	ws.DeleteLatestReport(uuid)
}

func ClearRecord(c *gin.Context) {
	tenantID, ok := currentTenantID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{
			"status":  "error",
			"message": "Tenant context is required",
		})
		return
	}
	if err := records.DeleteAllByTenant(tenantID); err != nil {
		c.JSON(500, gin.H{
			"status":  "error",
			"message": "Failed to delete Record" + err.Error(),
		})
		return
	}
	user_uuid, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, user_uuid.(string), "clear records", "warn")
	c.JSON(200, gin.H{"status": "success"})
}

func GetClient(c *gin.Context) {
	uuid := c.Param("uuid")
	tenantID, ok := currentTenantID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{
			"status":  "error",
			"message": "Tenant context is required",
		})
		return
	}
	if uuid == "" {
		c.JSON(400, gin.H{
			"status":  "error",
			"message": "Invalid or missing UUID",
		})
		return
	}

	result, err := clients.GetClientByUUIDForTenant(uuid, tenantID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"status":  "error",
				"message": "Client not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

func ListClients(c *gin.Context) {
	tenantID, ok := currentTenantID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "Tenant context is required"})
		return
	}
	cls, err := clients.GetAllClientBasicInfoByTenant(tenantID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cls)
}

func GetClientToken(c *gin.Context) {
	uuid := c.Param("uuid")
	tenantID, ok := currentTenantID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{
			"status":  "error",
			"message": "Tenant context is required",
		})
		return
	}
	if uuid == "" {
		c.JSON(400, gin.H{
			"status":  "error",
			"message": "Invalid or missing UUID",
		})
		return
	}

	token, err := clients.GetClientTokenByUUIDForTenant(uuid, tenantID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Client not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "token": token, "message:": ""})
}
