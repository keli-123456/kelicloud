package log

import (
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/auditlog"
)

func isPlatformAdminRequest(c *gin.Context) bool {
	if c == nil {
		return false
	}
	if _, ok := c.Get("api_key"); ok {
		return true
	}
	roleValue, ok := c.Get("user_role")
	if !ok {
		return false
	}
	role, _ := roleValue.(string)
	return accounts.IsUserRoleAtLeast(role, accounts.RoleAdmin)
}

func wantsAllLogs(c *gin.Context) bool {
	if c == nil {
		return false
	}
	scope := strings.ToLower(strings.TrimSpace(c.Query("scope")))
	if scope == "all" {
		return true
	}
	all := strings.ToLower(strings.TrimSpace(c.Query("all")))
	return all == "1" || all == "true"
}

func GetLogs(c *gin.Context) {
	limit := c.Query("limit")
	if limit == "" {
		limit = "100" // Default to 100 logs if not specified
	}
	page := c.Query("page")
	if page == "" {
		page = "1" // Default to page 1 if not specified
	}
	// Convert limit and page to integers
	// If conversion fails, return an error
	limitInt, err := strconv.Atoi(limit)
	if err != nil || limitInt <= 0 {
		api.RespondError(c, 400, "Invalid limit: "+limit)
		return
	}
	pageInt, err := strconv.Atoi(page)
	if err != nil || pageInt <= 0 {
		api.RespondError(c, 400, "Invalid page: "+page)
		return
	}
	userValue, ok := c.Get("uuid")
	userUUID, _ := userValue.(string)
	// 添加分页：计算偏移量并限制数量
	offset := (pageInt - 1) * limitInt

	var (
		logs  interface{}
		total int64
	)
	platformAdmin := isPlatformAdminRequest(c)
	responseScope := "self"
	targetUserUUID := strings.TrimSpace(c.Query("user_uuid"))

	switch {
	case wantsAllLogs(c):
		if !platformAdmin {
			api.RespondError(c, 403, "Platform admin permission is required")
			return
		}
		responseScope = "all"
		logs, total, err = auditlog.ListLogs(limitInt, offset)
	case targetUserUUID != "":
		if !platformAdmin && targetUserUUID != userUUID {
			api.RespondError(c, 403, "Platform admin permission is required")
			return
		}
		responseScope = "user"
		logs, total, err = auditlog.ListLogsByUser(targetUserUUID, limitInt, offset)
	case platformAdmin && (!ok || strings.TrimSpace(userUUID) == ""):
		responseScope = "all"
		logs, total, err = auditlog.ListLogs(limitInt, offset)
	default:
		if !ok || strings.TrimSpace(userUUID) == "" {
			api.RespondError(c, 403, "User context is required")
			return
		}
		logs, total, err = auditlog.ListLogsByUser(userUUID, limitInt, offset)
	}
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve logs: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"logs": logs, "total": total, "scope": responseScope})
}
