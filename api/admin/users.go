package admin

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func ListUsers(c *gin.Context) {
	users, err := accounts.ListUsers()
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list users: "+err.Error())
		return
	}
	items, err := buildAdminUserList(users)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to build user summaries: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{
		"items":              items,
		"available_features": config.UserAvailableFeatures(),
	})
}

func CreateUser(c *gin.Context) {
	var req struct {
		Username        string    `json:"username" binding:"required"`
		Password        string    `json:"password" binding:"required"`
		Role            string    `json:"role"`
		ServerQuota     *int      `json:"server_quota"`
		AllowedFeatures *[]string `json:"allowed_features"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	req.Username = strings.TrimSpace(req.Username)
	req.Role = strings.TrimSpace(req.Role)
	if req.Role == "" {
		req.Role = accounts.RoleUser
	}
	if len(req.Username) < 3 {
		api.RespondError(c, http.StatusBadRequest, "Username must be at least 3 characters long")
		return
	}
	if len(req.Password) < 6 {
		api.RespondError(c, http.StatusBadRequest, "Password must be at least 6 characters long")
		return
	}
	user, err := accounts.CreateAccountWithRole(req.Username, req.Password, req.Role)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to create user: "+err.Error())
		return
	}
	if err := config.SetUserPolicy(user.UUID, req.ServerQuota, req.AllowedFeatures); err != nil {
		_ = accounts.DeleteUserByUUID(user.UUID)
		api.RespondError(c, http.StatusBadRequest, "Failed to set user policy: "+err.Error())
		return
	}
	user.Passwd = ""
	user.Role = accounts.NormalizeUserRole(user.Role)
	policy, err := config.GetUserPolicy(user.UUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load user policy: "+err.Error())
		return
	}
	if actor, ok := c.Get("uuid"); ok {
		if actorUUID, ok := actor.(string); ok {
			api.AuditLogForCurrentUser(c, actorUUID, "create user:"+user.UUID, "info")
		}
	}
	api.RespondSuccess(c, buildAdminUserItem(user, policy, 0))
}

func DeleteUser(c *gin.Context) {
	targetUserUUID := strings.TrimSpace(c.Param("uuid"))
	if targetUserUUID == "" {
		api.RespondError(c, http.StatusBadRequest, "User UUID is required")
		return
	}

	currentUserUUID, _ := c.Get("uuid")
	if currentUserUUID == targetUserUUID {
		api.RespondError(c, http.StatusBadRequest, "You cannot delete your own account")
		return
	}

	if err := accounts.DeleteUserByUUID(targetUserUUID); err != nil {
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			api.RespondError(c, http.StatusNotFound, "User not found")
		case errors.Is(err, accounts.ErrCannotDeleteLastAdmin):
			api.RespondError(c, http.StatusBadRequest, err.Error())
		default:
			api.RespondError(c, http.StatusInternalServerError, "Failed to delete user: "+err.Error())
		}
		return
	}

	if actor, ok := currentUserUUID.(string); ok {
		api.AuditLogForCurrentUser(c, actor, "delete user:"+targetUserUUID, "warn")
	}
	api.RespondSuccess(c, gin.H{"uuid": targetUserUUID})
}

type adminUserItem struct {
	UUID            string           `json:"uuid"`
	Username        string           `json:"username"`
	Role            string           `json:"role"`
	SSOType         string           `json:"sso_type"`
	SSOID           string           `json:"sso_id"`
	CreatedAt       models.LocalTime `json:"created_at"`
	UpdatedAt       models.LocalTime `json:"updated_at"`
	ServerQuota     int              `json:"server_quota"`
	AllowedFeatures []string         `json:"allowed_features,omitempty"`
	ClientCount     int64            `json:"client_count"`
}

func buildAdminUserItem(user models.User, policy config.UserPolicy, clientCount int64) adminUserItem {
	return adminUserItem{
		UUID:            user.UUID,
		Username:        user.Username,
		Role:            accounts.NormalizeUserRole(user.Role),
		SSOType:         user.SSOType,
		SSOID:           user.SSOID,
		CreatedAt:       user.CreatedAt,
		UpdatedAt:       user.UpdatedAt,
		ServerQuota:     policy.ServerQuota,
		AllowedFeatures: policy.AllowedFeatures,
		ClientCount:     clientCount,
	}
}

func buildAdminUserList(users []models.User) ([]adminUserItem, error) {
	userUUIDs := make([]string, 0, len(users))
	for _, user := range users {
		userUUIDs = append(userUUIDs, user.UUID)
	}
	counts, err := clients.CountClientsByUsers(userUUIDs)
	if err != nil {
		return nil, err
	}

	items := make([]adminUserItem, 0, len(users))
	for _, user := range users {
		policy, err := config.GetUserPolicy(user.UUID)
		if err != nil {
			return nil, err
		}
		items = append(items, buildAdminUserItem(user, policy, counts[user.UUID]))
	}
	return items, nil
}
