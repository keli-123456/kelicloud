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
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	failoverv2svc "github.com/komari-monitor/komari/utils/failoverv2"
	"gorm.io/gorm"
)

type upsertFailoverV2SharePayload struct {
	Title        string     `json:"title"`
	Note         string     `json:"note"`
	AccessPolicy string     `json:"access_policy"`
	ExpiresAt    *time.Time `json:"expires_at"`
}

type failoverV2ShareView struct {
	Token          string `json:"token,omitempty"`
	ServiceID      uint   `json:"service_id"`
	ServiceName    string `json:"service_name"`
	Title          string `json:"title,omitempty"`
	Note           string `json:"note,omitempty"`
	AccessPolicy   string `json:"access_policy"`
	Status         string `json:"status"`
	ExpiresAt      string `json:"expires_at,omitempty"`
	LastAccessedAt string `json:"last_accessed_at,omitempty"`
	ConsumedAt     string `json:"consumed_at,omitempty"`
	AccessCount    int    `json:"access_count"`
	IsExpired      bool   `json:"is_expired"`
	IsConsumed     bool   `json:"is_consumed"`
	CreatedAt      string `json:"created_at,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
}

func buildFailoverV2ShareView(share *models.FailoverV2Share, service *models.FailoverV2Service) *failoverV2ShareView {
	now := time.Now().UTC()
	view := &failoverV2ShareView{
		AccessPolicy: failoverv2svc.ShareAccessPolicyPublic,
		Status:       failoverv2svc.ShareStatusNotShared,
	}
	if service != nil {
		view.ServiceID = service.ID
		view.ServiceName = strings.TrimSpace(service.Name)
	}
	if share == nil {
		return view
	}

	accessPolicy, err := failoverv2svc.NormalizeShareAccessPolicy(share.AccessPolicy)
	if err != nil {
		accessPolicy = failoverv2svc.ShareAccessPolicyPublic
	}

	view.Token = strings.TrimSpace(share.ShareToken)
	view.ServiceID = share.ServiceID
	if view.ServiceName == "" {
		view.ServiceName = strings.TrimSpace(share.ServiceName)
	}
	view.Title = strings.TrimSpace(share.Title)
	view.Note = strings.TrimSpace(share.Note)
	view.AccessPolicy = accessPolicy
	view.Status = failoverv2svc.ShareStatus(share, now)
	view.ExpiresAt = formatFailoverV2ShareTime(share.ExpiresAt)
	view.LastAccessedAt = formatFailoverV2ShareTime(share.LastAccessedAt)
	view.ConsumedAt = formatFailoverV2ShareTime(share.ConsumedAt)
	view.AccessCount = share.AccessCount
	view.IsExpired = failoverv2svc.IsShareExpired(share, now)
	view.IsConsumed = failoverv2svc.IsShareConsumed(share)
	view.CreatedAt = formatFailoverV2ShareTime(&share.CreatedAt)
	view.UpdatedAt = formatFailoverV2ShareTime(&share.UpdatedAt)
	return view
}

func formatFailoverV2ShareTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func GetFailoverV2Share(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}

	service, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID)
	if err != nil {
		respondFailoverV2ShareLoadError(c, err)
		return
	}

	share, err := failoverv2db.GetShareByServiceForUser(scope.UserUUID, service.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondSuccess(c, buildFailoverV2ShareView(nil, service))
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 share: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverV2ShareView(share, service))
}

func UpsertFailoverV2Share(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}

	var payload upsertFailoverV2SharePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid failover v2 share payload: "+err.Error())
		return
	}

	service, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID)
	if err != nil {
		respondFailoverV2ShareLoadError(c, err)
		return
	}

	accessPolicy, err := failoverv2svc.NormalizeShareAccessPolicy(payload.AccessPolicy)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	if payload.ExpiresAt != nil {
		expiresAt := payload.ExpiresAt.UTC()
		if !expiresAt.After(now) {
			api.RespondError(c, http.StatusBadRequest, "Failover v2 share expiration must be in the future")
			return
		}
		payload.ExpiresAt = &expiresAt
	}

	share, err := failoverv2db.GetShareByServiceForUser(scope.UserUUID, service.ID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 share: "+err.Error())
		return
	}
	if errors.Is(err, gorm.ErrRecordNotFound) || share == nil {
		share = &models.FailoverV2Share{
			UserID:    scope.UserUUID,
			ServiceID: service.ID,
		}
	}
	if strings.TrimSpace(share.ShareToken) == "" || failoverv2svc.IsShareConsumed(share) || failoverv2svc.IsShareExpired(share, now) {
		share.ShareToken = uuid.NewString()
		share.LastAccessedAt = nil
		share.ConsumedAt = nil
		share.AccessCount = 0
	}

	share.UserID = scope.UserUUID
	share.ServiceID = service.ID
	share.ServiceName = strings.TrimSpace(service.Name)
	share.Title = strings.TrimSpace(payload.Title)
	share.Note = strings.TrimSpace(payload.Note)
	share.AccessPolicy = accessPolicy
	share.ExpiresAt = payload.ExpiresAt

	if err := failoverv2db.SaveShare(share); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save failover v2 share: "+err.Error())
		return
	}

	auditFailoverV2ServiceAction(c, scope.UserUUID, "share", service.ID, "info")
	api.RespondSuccess(c, buildFailoverV2ShareView(share, service))
}

func DeleteFailoverV2Share(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}

	service, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID)
	if err != nil {
		respondFailoverV2ShareLoadError(c, err)
		return
	}

	share, err := failoverv2db.GetShareByServiceForUser(scope.UserUUID, service.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondSuccess(c, nil)
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 share: "+err.Error())
		return
	}

	if err := failoverv2db.DeleteShare(share); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete failover v2 share: "+err.Error())
		return
	}

	auditFailoverV2ServiceAction(c, scope.UserUUID, "revoke share", service.ID, "warn")
	api.RespondSuccess(c, nil)
}

func respondFailoverV2ShareLoadError(c *gin.Context, err error) {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
		return
	}
	api.RespondError(c, http.StatusInternalServerError, fmt.Sprintf("Failed to load failover v2 service: %v", err))
}
