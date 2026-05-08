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
	failoverdb "github.com/komari-monitor/komari/database/failover"
	"github.com/komari-monitor/komari/database/models"
	failoversvc "github.com/komari-monitor/komari/utils/failover"
	"gorm.io/gorm"
)

type upsertFailoverSharePayload struct {
	Title        string     `json:"title"`
	Note         string     `json:"note"`
	AccessPolicy string     `json:"access_policy"`
	ExpiresAt    *time.Time `json:"expires_at"`
}

type failoverShareView struct {
	Token          string `json:"token,omitempty"`
	TaskID         uint   `json:"task_id"`
	TaskName       string `json:"task_name"`
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

func buildFailoverShareView(share *models.FailoverShare, task *models.FailoverTask) *failoverShareView {
	now := time.Now().UTC()
	view := &failoverShareView{
		AccessPolicy: failoversvc.ShareAccessPolicyPublic,
		Status:       failoversvc.ShareStatusNotShared,
	}
	if task != nil {
		view.TaskID = task.ID
		view.TaskName = strings.TrimSpace(task.Name)
	}
	if share == nil {
		return view
	}

	accessPolicy, err := failoversvc.NormalizeShareAccessPolicy(share.AccessPolicy)
	if err != nil {
		accessPolicy = failoversvc.ShareAccessPolicyPublic
	}

	view.Token = strings.TrimSpace(share.ShareToken)
	view.TaskID = share.TaskID
	if view.TaskName == "" {
		view.TaskName = strings.TrimSpace(share.TaskName)
	}
	view.Title = strings.TrimSpace(share.Title)
	view.Note = strings.TrimSpace(share.Note)
	view.AccessPolicy = accessPolicy
	view.Status = failoversvc.ShareStatus(share, now)
	view.ExpiresAt = formatFailoverShareTime(share.ExpiresAt)
	view.LastAccessedAt = formatFailoverShareTime(share.LastAccessedAt)
	view.ConsumedAt = formatFailoverShareTime(share.ConsumedAt)
	view.AccessCount = share.AccessCount
	view.IsExpired = failoversvc.IsShareExpired(share, now)
	view.IsConsumed = failoversvc.IsShareConsumed(share)
	view.CreatedAt = formatFailoverShareTime(&share.CreatedAt)
	view.UpdatedAt = formatFailoverShareTime(&share.UpdatedAt)
	return view
}

func formatFailoverShareTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func GetFailoverShare(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	taskID, ok := parseFailoverTaskID(c, "id")
	if !ok {
		return
	}

	task, err := failoverdb.GetTaskByIDForUser(scope.UserUUID, taskID)
	if err != nil {
		respondFailoverShareLoadError(c, err)
		return
	}

	share, err := failoverdb.GetShareByTaskForUser(scope.UserUUID, task.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondSuccess(c, buildFailoverShareView(nil, task))
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover share: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverShareView(share, task))
}

func UpsertFailoverShare(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	taskID, ok := parseFailoverTaskID(c, "id")
	if !ok {
		return
	}

	var payload upsertFailoverSharePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid failover share payload: "+err.Error())
		return
	}

	task, err := failoverdb.GetTaskByIDForUser(scope.UserUUID, taskID)
	if err != nil {
		respondFailoverShareLoadError(c, err)
		return
	}

	accessPolicy, err := failoversvc.NormalizeShareAccessPolicy(payload.AccessPolicy)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	if payload.ExpiresAt != nil {
		expiresAt := payload.ExpiresAt.UTC()
		if !expiresAt.After(now) {
			api.RespondError(c, http.StatusBadRequest, "Failover share expiration must be in the future")
			return
		}
		payload.ExpiresAt = &expiresAt
	}

	share, err := failoverdb.GetShareByTaskForUser(scope.UserUUID, task.ID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover share: "+err.Error())
		return
	}
	if errors.Is(err, gorm.ErrRecordNotFound) || share == nil {
		share = &models.FailoverShare{
			UserID: scope.UserUUID,
			TaskID: task.ID,
		}
	}
	if strings.TrimSpace(share.ShareToken) == "" || failoversvc.IsShareConsumed(share) || failoversvc.IsShareExpired(share, now) {
		share.ShareToken = uuid.NewString()
		share.LastAccessedAt = nil
		share.ConsumedAt = nil
		share.AccessCount = 0
	}

	share.UserID = scope.UserUUID
	share.TaskID = task.ID
	share.TaskName = strings.TrimSpace(task.Name)
	share.Title = strings.TrimSpace(payload.Title)
	share.Note = strings.TrimSpace(payload.Note)
	share.AccessPolicy = accessPolicy
	share.ExpiresAt = payload.ExpiresAt

	if err := failoverdb.SaveShare(share); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save failover share: "+err.Error())
		return
	}

	auditFailoverTaskAction(c, scope.UserUUID, "share", task.ID, "info")
	api.RespondSuccess(c, buildFailoverShareView(share, task))
}

func DeleteFailoverShare(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	taskID, ok := parseFailoverTaskID(c, "id")
	if !ok {
		return
	}

	task, err := failoverdb.GetTaskByIDForUser(scope.UserUUID, taskID)
	if err != nil {
		respondFailoverShareLoadError(c, err)
		return
	}

	share, err := failoverdb.GetShareByTaskForUser(scope.UserUUID, task.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondSuccess(c, nil)
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover share: "+err.Error())
		return
	}

	if err := failoverdb.DeleteShare(share); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete failover share: "+err.Error())
		return
	}

	auditFailoverTaskAction(c, scope.UserUUID, "revoke share", task.ID, "warn")
	api.RespondSuccess(c, nil)
}

func respondFailoverShareLoadError(c *gin.Context, err error) {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		api.RespondError(c, http.StatusNotFound, "Failover task not found")
		return
	}
	api.RespondError(c, http.StatusInternalServerError, fmt.Sprintf("Failed to load failover task: %v", err))
}
