package admin

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database"
	"gorm.io/gorm"
)

func GetCurrentTenantInvites(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}
	if !database.IsTenantRoleAtLeast(currentTenantRole(c), database.RoleAdmin) {
		api.RespondError(c, http.StatusForbidden, "Tenant admin permission is required")
		return
	}

	invites, err := database.ListTenantInvites(tenantID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list tenant invites: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"items": invites})
}

func CreateCurrentTenantInvite(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}
	currentRole := currentTenantRole(c)
	if !database.IsTenantRoleAtLeast(currentRole, database.RoleAdmin) {
		api.RespondError(c, http.StatusForbidden, "Tenant admin permission is required")
		return
	}

	var payload struct {
		Role           string `json:"role"`
		ExpiresInHours int    `json:"expires_in_hours"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil && !errors.Is(err, io.EOF) {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	targetRole := database.NormalizeTenantRole(strings.TrimSpace(payload.Role))
	if targetRole == database.RoleOwner && currentRole != database.RoleOwner {
		api.RespondError(c, http.StatusForbidden, "Only tenant owners can create owner invites")
		return
	}

	expiresInHours := payload.ExpiresInHours
	if expiresInHours <= 0 {
		expiresInHours = 72
	}
	expiresAt := time.Now().Add(time.Duration(expiresInHours) * time.Hour)

	userUUID, _ := c.Get("uuid")
	invite, err := database.CreateTenantInvite(tenantID, userUUID.(string), targetRole, &expiresAt)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to create tenant invite: "+err.Error())
		return
	}

	info, err := database.GetTenantInviteByToken(invite.Token)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load tenant invite: "+err.Error())
		return
	}

	api.AuditLogForCurrentTenant(c, userUUID.(string), "create tenant invite: "+invite.ID+" as "+targetRole, "info")
	api.RespondSuccess(c, info)
}

func RevokeCurrentTenantInvite(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}
	if !database.IsTenantRoleAtLeast(currentTenantRole(c), database.RoleAdmin) {
		api.RespondError(c, http.StatusForbidden, "Tenant admin permission is required")
		return
	}

	inviteID := strings.TrimSpace(c.Param("invite_id"))
	if inviteID == "" {
		api.RespondError(c, http.StatusBadRequest, "invite_id is required")
		return
	}

	if err := database.RevokeTenantInvite(tenantID, inviteID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Tenant invite not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to revoke tenant invite: "+err.Error())
		return
	}

	userUUID, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, userUUID.(string), "revoke tenant invite: "+inviteID, "warn")
	api.RespondSuccess(c, gin.H{"id": inviteID})
}
