package clipboard

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	clipboardDB "github.com/komari-monitor/komari/database/clipboard"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func tenantIDFromContext(c *gin.Context) (string, bool) {
	tenantID, ok := c.Get("tenant_id")
	if !ok {
		return "", false
	}
	value, ok := tenantID.(string)
	return value, ok && value != ""
}

// GetClipboard retrieves a clipboard entry by ID
func GetClipboard(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid ID")
		return
	}
	tenantID, ok := tenantIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}
	cb, err := clipboardDB.GetClipboardByIDForTenant(id, tenantID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Clipboard not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to get clipboard: "+err.Error())
		return
	}
	api.RespondSuccess(c, cb)
}

// ListClipboard lists all clipboard entries
func ListClipboard(c *gin.Context) {
	tenantID, ok := tenantIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}
	list, err := clipboardDB.ListClipboardByTenant(tenantID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list clipboard: "+err.Error())
		return
	}
	api.RespondSuccess(c, list)
}

// CreateClipboard creates a new clipboard entry
func CreateClipboard(c *gin.Context) {
	var req models.Clipboard
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	tenantID, ok := tenantIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}
	req.Id = 0
	req.TenantID = tenantID
	if err := clipboardDB.CreateClipboardForTenant(tenantID, &req); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to create clipboard: "+err.Error())
		return
	}
	userUUID, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, userUUID.(string), "create clipboard:"+strconv.Itoa(req.Id), "info")
	api.RespondSuccess(c, req)
}

// UpdateClipboard updates an existing clipboard entry
func UpdateClipboard(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid ID")
		return
	}
	var fields map[string]interface{}
	if err := c.ShouldBindJSON(&fields); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	tenantID, ok := tenantIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}
	if err := clipboardDB.UpdateClipboardFieldsForTenant(id, tenantID, fields); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Clipboard not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to update clipboard: "+err.Error())
		return
	}
	userUUID, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, userUUID.(string), "update clipboard:"+strconv.Itoa(id), "info")
	api.RespondSuccess(c, nil)
}

// DeleteClipboard deletes a clipboard entry
func DeleteClipboard(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid ID")
		return
	}
	tenantID, ok := tenantIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}
	if err := clipboardDB.DeleteClipboardForTenant(id, tenantID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Clipboard not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete clipboard: "+err.Error())
		return
	}
	userUUID, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, userUUID.(string), "delete clipboard:"+strconv.Itoa(id), "warn")
	api.RespondSuccess(c, nil)
}

// BatchDeleteClipboard deletes multiple clipboard entries
func BatchDeleteClipboard(c *gin.Context) {
	var req struct {
		IDs []int `json:"ids" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	if len(req.IDs) == 0 {
		api.RespondError(c, http.StatusBadRequest, "IDs cannot be empty")
		return
	}
	tenantID, ok := tenantIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}
	if err := clipboardDB.DeleteClipboardBatchForTenant(req.IDs, tenantID); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to batch delete clipboard: "+err.Error())
		return
	}
	userUUID, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, userUUID.(string), "batch delete clipboard: "+strconv.Itoa(len(req.IDs))+" items", "warn")
	api.RespondSuccess(c, nil)
}
