package clipboard

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	clipboardDB "github.com/komari-monitor/komari/database/clipboard"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func userUUIDFromContext(c *gin.Context) (string, bool) {
	userUUID, ok := c.Get("uuid")
	if !ok {
		return "", false
	}
	value, ok := userUUID.(string)
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
	userUUID, ok := userUUIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}
	cb, err := clipboardDB.GetClipboardByIDForUser(id, userUUID)
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
	userUUID, ok := userUUIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}

	rawPage := strings.TrimSpace(c.Query("page"))
	rawLimit := strings.TrimSpace(c.Query("limit"))
	rawSearch := strings.TrimSpace(c.Query("search"))
	if rawPage != "" || rawLimit != "" || rawSearch != "" {
		page := 1
		limit := 20

		if rawPage != "" {
			parsed, err := strconv.Atoi(rawPage)
			if err != nil || parsed <= 0 {
				api.RespondError(c, http.StatusBadRequest, "Invalid page")
				return
			}
			page = parsed
		}
		if rawLimit != "" {
			parsed, err := strconv.Atoi(rawLimit)
			if err != nil || parsed <= 0 {
				api.RespondError(c, http.StatusBadRequest, "Invalid limit")
				return
			}
			limit = parsed
		}

		items, total, err := clipboardDB.ListClipboardPageByUserWithSearch(userUUID, page, limit, rawSearch)
		if err != nil {
			api.RespondError(c, http.StatusInternalServerError, "Failed to list clipboard: "+err.Error())
			return
		}
		api.RespondSuccess(c, gin.H{
			"items": items,
			"total": total,
			"page":  page,
			"limit": limit,
		})
		return
	}

	list, err := clipboardDB.ListClipboardByUser(userUUID)
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
	userUUID, ok := userUUIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}
	req.Id = 0
	req.UserID = userUUID
	if err := clipboardDB.CreateClipboardForUser(userUUID, &req); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to create clipboard: "+err.Error())
		return
	}
	api.AuditLogForCurrentUser(c, userUUID, "create clipboard:"+strconv.Itoa(req.Id), "info")
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
	userUUID, ok := userUUIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}
	err = clipboardDB.UpdateClipboardFieldsForUser(id, userUUID, fields)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Clipboard not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to update clipboard: "+err.Error())
		return
	}
	api.AuditLogForCurrentUser(c, userUUID, "update clipboard:"+strconv.Itoa(id), "info")
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
	userUUID, ok := userUUIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}
	err = clipboardDB.DeleteClipboardForUser(id, userUUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Clipboard not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete clipboard: "+err.Error())
		return
	}
	api.AuditLogForCurrentUser(c, userUUID, "delete clipboard:"+strconv.Itoa(id), "warn")
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
	userUUID, ok := userUUIDFromContext(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}
	err := clipboardDB.DeleteClipboardBatchForUser(req.IDs, userUUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to batch delete clipboard: "+err.Error())
		return
	}
	api.AuditLogForCurrentUser(c, userUUID, "batch delete clipboard: "+strconv.Itoa(len(req.IDs))+" items", "warn")
	api.RespondSuccess(c, nil)
}
