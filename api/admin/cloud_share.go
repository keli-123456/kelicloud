package admin

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/cloudshare"
	"gorm.io/gorm"
)

type upsertCloudInstanceSharePayload struct {
	Title              string     `json:"title"`
	Note               string     `json:"note"`
	AccessPolicy       string     `json:"access_policy"`
	ExpiresAt          *time.Time `json:"expires_at"`
	SharePassword      bool       `json:"share_password"`
	ShareManagedSSHKey bool       `json:"share_managed_ssh_key"`
}

func GetCloudInstanceShare(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	provider, resourceType, resourceID, err := cloudshare.NormalizeReference(
		c.Param("provider"),
		c.Param("resource_type"),
		c.Param("resource_id"),
	)
	if err != nil {
		respondCloudShareError(c, err)
		return
	}
	if !ensureCloudProviderFeatureAllowed(c, provider) {
		return
	}

	state, err := resolveCloudResourceForScope(scope, provider, resourceType, resourceID)
	if err != nil {
		respondCloudShareError(c, err)
		return
	}

	share, err := getCloudInstanceShareForScope(scope, provider, resourceType, resourceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondSuccess(c, cloudshare.BuildAdminShareView(nil, state))
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load cloud instance share: "+err.Error())
		return
	}

	api.RespondSuccess(c, cloudshare.BuildAdminShareView(share, state))
}

func UpsertCloudInstanceShare(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	provider, resourceType, resourceID, err := cloudshare.NormalizeReference(
		c.Param("provider"),
		c.Param("resource_type"),
		c.Param("resource_id"),
	)
	if err != nil {
		respondCloudShareError(c, err)
		return
	}
	if !ensureCloudProviderFeatureAllowed(c, provider) {
		return
	}

	var payload upsertCloudInstanceSharePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid cloud share payload: "+err.Error())
		return
	}

	state, err := resolveCloudResourceForScope(scope, provider, resourceType, resourceID)
	if err != nil {
		respondCloudShareError(c, err)
		return
	}

	if payload.SharePassword && !state.CanSharePassword {
		api.RespondError(c, http.StatusBadRequest, "This instance does not have a saved root password to share")
		return
	}
	if payload.ShareManagedSSHKey && !state.CanShareManagedSSHKey {
		api.RespondError(c, http.StatusBadRequest, "This instance does not have a managed SSH key to share")
		return
	}
	accessPolicy, err := cloudshare.NormalizeAccessPolicy(payload.AccessPolicy)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	if payload.ExpiresAt != nil {
		expiresAt := payload.ExpiresAt.UTC()
		if !expiresAt.After(now) {
			api.RespondError(c, http.StatusBadRequest, "Cloud share expiration must be in the future")
			return
		}
		payload.ExpiresAt = &expiresAt
	}

	share, err := getCloudInstanceShareForScope(scope, provider, resourceType, resourceID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load cloud instance share: "+err.Error())
		return
	}
	if errors.Is(err, gorm.ErrRecordNotFound) || share == nil {
		share = &models.CloudInstanceShare{
			UserID:       scope.UserUUID,
			Provider:     provider,
			ResourceType: resourceType,
			ResourceID:   resourceID,
		}
	}
	if strings.TrimSpace(share.ShareToken) == "" || cloudshare.IsConsumed(share) || cloudshare.IsExpired(share, now) {
		share.ShareToken = uuid.NewString()
		share.ConsumedAt = nil
		share.LastAccessedAt = nil
		share.AccessCount = 0
	}

	share.UserID = scope.UserUUID
	share.Provider = provider
	share.ResourceType = resourceType
	share.ResourceID = resourceID
	share.ResourceName = strings.TrimSpace(state.ResourceName)
	share.CredentialID = strings.TrimSpace(state.CredentialID)
	share.Region = strings.TrimSpace(state.Region)
	share.Title = strings.TrimSpace(payload.Title)
	share.Note = strings.TrimSpace(payload.Note)
	share.AccessPolicy = accessPolicy
	share.ExpiresAt = payload.ExpiresAt
	share.SharePassword = payload.SharePassword
	share.ShareManagedSSHKey = payload.ShareManagedSSHKey

	if err := database.SaveCloudInstanceShare(share); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save cloud instance share: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("share %s %s %s", provider, resourceType, resourceID))
	api.RespondSuccess(c, cloudshare.BuildAdminShareView(share, state))
}

func DeleteCloudInstanceShare(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	provider, resourceType, resourceID, err := cloudshare.NormalizeReference(
		c.Param("provider"),
		c.Param("resource_type"),
		c.Param("resource_id"),
	)
	if err != nil {
		respondCloudShareError(c, err)
		return
	}
	if !ensureCloudProviderFeatureAllowed(c, provider) {
		return
	}

	share, err := getCloudInstanceShareForScope(scope, provider, resourceType, resourceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondSuccess(c, nil)
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load cloud instance share: "+err.Error())
		return
	}

	if err := database.DeleteCloudInstanceShare(share); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete cloud instance share: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("revoke share %s %s %s", provider, resourceType, resourceID))
	api.RespondSuccess(c, nil)
}

func respondCloudShareError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, cloudshare.ErrInvalidReference), errors.Is(err, cloudshare.ErrUnsupportedCapability), errors.Is(err, cloudshare.ErrInvalidAccessPolicy):
		api.RespondError(c, http.StatusBadRequest, err.Error())
	case errors.Is(err, cloudshare.ErrInstanceNotFound), errors.Is(err, cloudshare.ErrCredentialNotFound):
		api.RespondError(c, http.StatusNotFound, err.Error())
	case errors.Is(err, cloudshare.ErrProviderNotConfigured):
		api.RespondError(c, http.StatusBadRequest, err.Error())
	default:
		api.RespondError(c, http.StatusInternalServerError, err.Error())
	}
}
