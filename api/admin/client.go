package admin

import (
	"errors"
	"net/http"
	"strings"

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
		Name     string `json:"name"`
		UserUUID string `json:"user_uuid"`
	}
	userUUID, ok := currentUserUUID(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "User context is required"})
		return
	}
	targetUserUUID := userUUID
	platformAdmin, _ := isPlatformAdmin(c)
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		if requested := strings.TrimSpace(req.UserUUID); requested != "" {
			if !platformAdmin && requested != userUUID {
				c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "Platform admin permission is required"})
				return
			}
			targetUserUUID = requested
		}
		uuid, token, err := clients.CreateClientForUser(targetUserUUID)
		if err != nil {
			if errors.Is(err, clients.ErrClientQuotaExceeded) {
				c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": err.Error()})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "success", "uuid": uuid, "token": token})
		return
	}
	if requested := strings.TrimSpace(req.UserUUID); requested != "" {
		if !platformAdmin && requested != userUUID {
			c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "Platform admin permission is required"})
			return
		}
		targetUserUUID = requested
	}
	uuid, token, err := clients.CreateClientWithNameForUser(targetUserUUID, req.Name)
	if err != nil {
		if errors.Is(err, clients.ErrClientQuotaExceeded) {
			c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": err.Error()})
			return
		}
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
	platformAdmin, _ := isPlatformAdmin(c)
	var err error
	if platformAdmin {
		err = clients.SaveClient(req)
	} else {
		err = clients.SaveClientForUser(userUUID, req)
	}
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
	platformAdmin, _ := isPlatformAdmin(c)
	var err error
	if platformAdmin {
		err = clients.DeleteClient(uuid)
	} else {
		err = clients.DeleteClientForUser(userUUID, uuid)
	}
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
	platformAdmin, _ := isPlatformAdmin(c)
	if platformAdmin {
		result, err = clients.GetClientByUUID(uuid)
	} else {
		result, err = clients.GetClientByUUIDForUser(uuid, userUUID)
	}
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
	platformAdmin, _ := isPlatformAdmin(c)
	targetUserUUID := strings.TrimSpace(c.Query("user_uuid"))
	listAll := strings.EqualFold(strings.TrimSpace(c.Query("all")), "1") || strings.EqualFold(strings.TrimSpace(c.Query("all")), "true")

	var (
		cls []models.Client
		err error
	)
	switch {
	case platformAdmin && listAll:
		cls, err = clients.GetAllClientBasicInfo()
	case targetUserUUID != "":
		if !platformAdmin && targetUserUUID != userUUID {
			c.JSON(http.StatusForbidden, gin.H{"status": "error", "message": "Platform admin permission is required"})
			return
		}
		cls, err = clients.GetAllClientBasicInfoByUser(targetUserUUID)
	default:
		cls, err = clients.GetAllClientBasicInfoByUser(userUUID)
	}
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
	platformAdmin, _ := isPlatformAdmin(c)
	if platformAdmin {
		token, err = clients.GetClientTokenByUUID(uuid)
	} else {
		token, err = clients.GetClientTokenByUUIDForUser(uuid, userUUID)
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Client not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	api.AuditLogForCurrentUser(c, userUUID, "view client token:"+uuid, "warn")
	c.JSON(http.StatusOK, gin.H{"status": "success", "token": token, "message:": ""})
}
