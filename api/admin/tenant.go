package admin

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/accounts"
	"gorm.io/gorm"
)

func GetAccessibleTenants(c *gin.Context) {
	userUUID, _ := c.Get("uuid")
	currentTenantID, _ := c.Get("tenant_id")

	currentTenant, tenantList, err := database.ResolveAccessibleTenant(
		userUUID.(string),
		stringValue(currentTenantID),
	)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load tenants: "+err.Error())
		return
	}

	api.RespondSuccess(c, gin.H{
		"current": currentTenant,
		"items":   tenantList,
	})
}

func SwitchCurrentTenant(c *gin.Context) {
	var payload struct {
		TenantID string `json:"tenant_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	userUUID, _ := c.Get("uuid")
	session, _ := c.Get("session")

	tenant, err := database.GetAccessibleTenantByUser(userUUID.(string), payload.TenantID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusForbidden, "Tenant not accessible.")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to switch tenant: "+err.Error())
		return
	}

	if err := accounts.SetSessionCurrentTenant(session.(string), tenant.ID); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update current tenant: "+err.Error())
		return
	}

	api.AuditLogForTenant(tenant.ID, c.ClientIP(), userUUID.(string), "switch tenant: "+tenant.ID, "info")
	api.RespondSuccess(c, gin.H{"current": tenant})
}

func CreateTenant(c *gin.Context) {
	var payload struct {
		Name        string `json:"name" binding:"required"`
		Slug        string `json:"slug"`
		Description string `json:"description"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	userUUID, _ := c.Get("uuid")
	tenant, err := database.CreateTenant(payload.Name, payload.Slug, payload.Description, userUUID.(string))
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to create tenant: "+err.Error())
		return
	}

	api.AuditLogForTenant(tenant.ID, c.ClientIP(), userUUID.(string), "create tenant: "+tenant.ID, "info")
	api.RespondSuccess(c, tenant)
}

func GetCurrentTenantMembers(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	members, err := database.ListTenantMembers(tenantID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list tenant members: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"items": members})
}

func AddCurrentTenantMember(c *gin.Context) {
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
		UserUUID string `json:"user_uuid"`
		Username string `json:"username"`
		Role     string `json:"role"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	targetUserUUID := strings.TrimSpace(payload.UserUUID)
	if targetUserUUID == "" {
		if strings.TrimSpace(payload.Username) == "" {
			api.RespondError(c, http.StatusBadRequest, "user_uuid or username is required")
			return
		}
		user, err := accounts.GetUserByUsername(strings.TrimSpace(payload.Username))
		if err != nil {
			api.RespondError(c, http.StatusNotFound, "User not found: "+err.Error())
			return
		}
		targetUserUUID = user.UUID
	}

	targetRole := database.NormalizeTenantRole(strings.TrimSpace(payload.Role))
	if targetRole == database.RoleOwner && currentRole != database.RoleOwner {
		api.RespondError(c, http.StatusForbidden, "Only tenant owners can add another owner")
		return
	}

	if err := database.AddTenantMember(tenantID, targetUserUUID, targetRole); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to add tenant member: "+err.Error())
		return
	}

	userUUID, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, userUUID.(string), "add tenant member: "+targetUserUUID+" as "+targetRole, "info")
	api.RespondSuccess(c, gin.H{"user_uuid": targetUserUUID, "role": targetRole})
}

func UpdateCurrentTenantMemberRole(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}
	currentRole := currentTenantRole(c)
	if !database.IsTenantRoleAtLeast(currentRole, database.RoleAdmin) {
		api.RespondError(c, http.StatusForbidden, "Tenant admin permission is required")
		return
	}

	targetUserUUID := c.Param("user_uuid")
	if strings.TrimSpace(targetUserUUID) == "" {
		api.RespondError(c, http.StatusBadRequest, "user_uuid is required")
		return
	}

	targetMember, err := database.GetTenantMember(tenantID, targetUserUUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Tenant member not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load tenant member: "+err.Error())
		return
	}

	var payload struct {
		Role string `json:"role" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	targetRole := database.NormalizeTenantRole(payload.Role)

	if currentRole != database.RoleOwner {
		if targetMember.Role == database.RoleOwner || targetRole == database.RoleOwner {
			api.RespondError(c, http.StatusForbidden, "Only tenant owners can manage owner roles")
			return
		}
	}

	if targetMember.Role == database.RoleOwner && targetRole != database.RoleOwner {
		ownerCount, err := database.CountTenantMembersByRole(tenantID, database.RoleOwner)
		if err != nil {
			api.RespondError(c, http.StatusInternalServerError, "Failed to count tenant owners: "+err.Error())
			return
		}
		if ownerCount <= 1 {
			api.RespondError(c, http.StatusBadRequest, "Cannot demote the last tenant owner")
			return
		}
	}

	if err := database.UpdateTenantMemberRole(tenantID, targetUserUUID, targetRole); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update tenant member role: "+err.Error())
		return
	}

	userUUID, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, userUUID.(string), "update tenant member role: "+targetUserUUID+" -> "+targetRole, "info")
	api.RespondSuccess(c, gin.H{"user_uuid": targetUserUUID, "role": targetRole})
}

func RemoveCurrentTenantMember(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}
	currentRole := currentTenantRole(c)
	if !database.IsTenantRoleAtLeast(currentRole, database.RoleAdmin) {
		api.RespondError(c, http.StatusForbidden, "Tenant admin permission is required")
		return
	}

	targetUserUUID := c.Param("user_uuid")
	if strings.TrimSpace(targetUserUUID) == "" {
		api.RespondError(c, http.StatusBadRequest, "user_uuid is required")
		return
	}

	targetMember, err := database.GetTenantMember(tenantID, targetUserUUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Tenant member not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load tenant member: "+err.Error())
		return
	}

	if currentRole != database.RoleOwner && targetMember.Role == database.RoleOwner {
		api.RespondError(c, http.StatusForbidden, "Only tenant owners can remove another owner")
		return
	}

	if targetMember.Role == database.RoleOwner {
		ownerCount, err := database.CountTenantMembersByRole(tenantID, database.RoleOwner)
		if err != nil {
			api.RespondError(c, http.StatusInternalServerError, "Failed to count tenant owners: "+err.Error())
			return
		}
		if ownerCount <= 1 {
			api.RespondError(c, http.StatusBadRequest, "Cannot remove the last tenant owner")
			return
		}
	}

	if err := database.DeleteTenantMember(tenantID, targetUserUUID); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to remove tenant member: "+err.Error())
		return
	}

	userUUID, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, userUUID.(string), "remove tenant member: "+targetUserUUID, "warn")
	api.RespondSuccess(c, gin.H{"user_uuid": targetUserUUID})
}

func currentTenantRole(c *gin.Context) string {
	role, _ := c.Get("tenant_role")
	if text, ok := role.(string); ok {
		return database.NormalizeTenantRole(text)
	}
	return database.RoleViewer
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
