package update

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	adminapi "github.com/komari-monitor/komari/api/admin"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/accounts"
)

func UpdateUser(c *gin.Context) {
	var req struct {
		Uuid            string    `json:"uuid" binding:"required"`
		Name            *string   `json:"username"`
		Password        *string   `json:"password"`
		SsoType         *string   `json:"sso_type"`
		Role            *string   `json:"role"`
		ServerQuota     *int      `json:"server_quota"`
		AllowedFeatures *[]string `json:"allowed_features"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, 400, "Invalid or missing request body: "+err.Error())
		return
	}
	if req.Password == nil && req.Name == nil && req.SsoType == nil && req.Role == nil && req.ServerQuota == nil && req.AllowedFeatures == nil {
		api.RespondError(c, 400, "At least one field must be provided")
		return
	}
	if req.Name != nil && len(*req.Name) < 3 {
		api.RespondError(c, 400, "Username must be at least 3 characters long")
		return
	}
	if req.Password != nil && len(*req.Password) < 6 {
		api.RespondError(c, 400, "Password must be at least 6 characters long")
		return
	}

	currentUUID, _ := c.Get("uuid")
	currentUserUUID := ""
	if currentUUID != nil {
		currentUserUUID, _ = currentUUID.(string)
	}
	isSelf := currentUserUUID == req.Uuid
	if !isSelf {
		if !adminapi.EnsurePlatformAdmin(c) {
			return
		}
	}
	if isSelf && req.Role != nil {
		api.RespondError(c, 403, "You cannot change your own role")
		return
	}
	if err := accounts.UpdateUser(req.Uuid, req.Name, req.Password, req.SsoType, req.Role); err != nil {
		api.RespondError(c, 500, "Failed to update user: "+err.Error())
		return
	}
	if err := config.SetUserPolicy(req.Uuid, req.ServerQuota, req.AllowedFeatures); err != nil {
		api.RespondError(c, 500, "Failed to update user policy: "+err.Error())
		return
	}
	if uuid, ok := c.Get("uuid"); ok {
		if actorUUID, ok := uuid.(string); ok {
			api.AuditLogForCurrentUser(c, actorUUID, "User updated", "warn")
		}
	}
	api.RespondSuccess(c, gin.H{"uuid": req.Uuid})
}
