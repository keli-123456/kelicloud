package admin

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/records"
	"github.com/komari-monitor/komari/ws"
	"gorm.io/gorm"
)

func currentUserUUID(c *gin.Context) (string, bool) {
	userUUID, ok := c.Get("uuid")
	if !ok {
		return "", false
	}
	value, ok := userUUID.(string)
	return value, ok && value != ""
}

func AddClient(c *gin.Context) {
	var req struct {
		Name string `json:"name"`
	}
	userUUID, ok := currentUserUUID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "User context is required"})
		return
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		uuid, token, err := clients.CreateClientForUser(userUUID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "success", "uuid": uuid, "token": token})
		return
	}
	uuid, token, err := clients.CreateClientWithNameForUser(userUUID, req.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	api.AuditLogForCurrentUser(c, userUUID, "create client:"+uuid, "info")
	c.JSON(http.StatusOK, gin.H{"status": "success", "uuid": uuid, "token": token, "message": ""})
}

func EditClient(c *gin.Context) {
	var req = make(map[string]interface{})
	uuid := c.Param("uuid")
	userUUID, ok := currentUserUUID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "User context is required"})
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
	err := clients.SaveClientForUser(userUUID, req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Client not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	api.AuditLogForCurrentUser(c, userUUID, "edit client:"+uuid, "info")
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func RemoveClient(c *gin.Context) {
	uuid := c.Param("uuid")
	userUUID, ok := currentUserUUID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "User context is required"})
		return
	}
	err := clients.DeleteClientForUser(userUUID, uuid)
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
	api.AuditLogForCurrentUser(c, userUUID, "delete client:"+uuid, "warn")
	c.JSON(200, gin.H{"status": "success"})
	ws.DeleteConnectedClients(uuid)
	ws.DeleteLatestReport(uuid)
}

func ClearRecord(c *gin.Context) {
	userUUID, ok := currentUserUUID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{
			"status":  "error",
			"message": "User context is required",
		})
		return
	}
	err := records.DeleteAllByUser(userUUID)
	if err != nil {
		c.JSON(500, gin.H{
			"status":  "error",
			"message": "Failed to delete Record" + err.Error(),
		})
		return
	}
	api.AuditLogForCurrentUser(c, userUUID, "clear records", "warn")
	c.JSON(200, gin.H{"status": "success"})
}

func GetClient(c *gin.Context) {
	uuid := c.Param("uuid")
	userUUID, ok := currentUserUUID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{
			"status":  "error",
			"message": "User context is required",
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

	var (
		result models.Client
		err    error
	)
	result, err = clients.GetClientByUUIDForUser(uuid, userUUID)
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
	userUUID, ok := currentUserUUID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "User context is required"})
		return
	}
	cls, err := clients.GetAllClientBasicInfoByUser(userUUID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cls)
}

func GetClientToken(c *gin.Context) {
	uuid := c.Param("uuid")
	userUUID, ok := currentUserUUID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{
			"status":  "error",
			"message": "User context is required",
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

	var (
		token string
		err   error
	)
	token, err = clients.GetClientTokenByUUIDForUser(uuid, userUUID)
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
