package admin

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/accounts"
	"gorm.io/gorm"
)

func ListUsers(c *gin.Context) {
	users, err := accounts.ListUsers()
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list users: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"items": users})
}

func CreateUser(c *gin.Context) {
	var req struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
		Role     string `json:"role"`
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
	user.Passwd = ""
	user.Role = accounts.NormalizeUserRole(user.Role)
	if actor, ok := c.Get("uuid"); ok {
		if actorUUID, ok := actor.(string); ok {
			api.AuditLogForCurrentUser(c, actorUUID, "create user:"+user.UUID, "info")
		}
	}
	api.RespondSuccess(c, user)
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
