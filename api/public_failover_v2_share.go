package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	failoverv2svc "github.com/komari-monitor/komari/utils/failoverv2"
	"gorm.io/gorm"
)

type publicFailoverV2ShareView struct {
	Token        string                      `json:"token"`
	Title        string                      `json:"title,omitempty"`
	Note         string                      `json:"note,omitempty"`
	AccessPolicy string                      `json:"access_policy"`
	ExpiresAt    *time.Time                  `json:"expires_at,omitempty"`
	CreatedAt    time.Time                   `json:"created_at"`
	UpdatedAt    time.Time                   `json:"updated_at"`
	GeneratedAt  time.Time                   `json:"generated_at"`
	Service      publicFailoverV2ServiceView `json:"service"`
}

type publicFailoverV2ServiceView struct {
	ID                 uint                                   `json:"id"`
	Name               string                                 `json:"name"`
	Enabled            bool                                   `json:"enabled"`
	DNSProvider        string                                 `json:"dns_provider"`
	LastExecutionID    *uint                                  `json:"last_execution_id,omitempty"`
	LastStatus         string                                 `json:"last_status"`
	LastMessage        string                                 `json:"last_message"`
	LastCheckedAt      *models.LocalTime                      `json:"last_checked_at"`
	MemberCount        int                                    `json:"member_count"`
	EnabledMemberCount int                                    `json:"enabled_member_count"`
	Members            []publicFailoverV2MemberView           `json:"members"`
	RecentExecutions   []publicFailoverV2ExecutionSummaryView `json:"recent_executions"`
	CreatedAt          models.LocalTime                       `json:"created_at"`
	UpdatedAt          models.LocalTime                       `json:"updated_at"`
}

type publicFailoverV2MemberView struct {
	ID                  uint              `json:"id"`
	ServiceID           uint              `json:"service_id"`
	Name                string            `json:"name"`
	Enabled             bool              `json:"enabled"`
	Priority            int               `json:"priority"`
	Mode                string            `json:"mode"`
	DNSLines            []string          `json:"dns_lines"`
	CurrentAddress      string            `json:"current_address"`
	Provider            string            `json:"provider"`
	TriggerFailureCount int               `json:"trigger_failure_count"`
	LastExecutionID     *uint             `json:"last_execution_id,omitempty"`
	LastStatus          string            `json:"last_status"`
	LastMessage         string            `json:"last_message"`
	LastTriggeredAt     *models.LocalTime `json:"last_triggered_at"`
	LastSucceededAt     *models.LocalTime `json:"last_succeeded_at"`
	LastFailedAt        *models.LocalTime `json:"last_failed_at"`
	CreatedAt           models.LocalTime  `json:"created_at"`
	UpdatedAt           models.LocalTime  `json:"updated_at"`
}

type publicFailoverV2ExecutionSummaryView struct {
	ID              uint              `json:"id"`
	ServiceID       uint              `json:"service_id"`
	MemberID        uint              `json:"member_id"`
	Status          string            `json:"status"`
	TriggerReason   string            `json:"trigger_reason"`
	DetachDNSStatus string            `json:"detach_dns_status"`
	AttachDNSStatus string            `json:"attach_dns_status"`
	CleanupStatus   string            `json:"cleanup_status"`
	ErrorMessage    string            `json:"error_message"`
	StartedAt       models.LocalTime  `json:"started_at"`
	FinishedAt      *models.LocalTime `json:"finished_at"`
}

func GetPublicFailoverV2Share(c *gin.Context) {
	token := strings.TrimSpace(c.Param("token"))
	if token == "" {
		RespondError(c, http.StatusBadRequest, "Invalid failover v2 share token")
		return
	}

	share, err := failoverv2db.GetShareByToken(token)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			RespondError(c, http.StatusNotFound, "Failover v2 share not found")
			return
		}
		RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 share")
		return
	}

	now := time.Now().UTC()
	if err := failoverv2svc.ValidateSharePublicAccess(share, now); err != nil {
		switch {
		case errors.Is(err, failoverv2svc.ErrShareExpired), errors.Is(err, failoverv2svc.ErrShareConsumed):
			RespondError(c, http.StatusGone, err.Error())
		case errors.Is(err, failoverv2svc.ErrShareNotFound):
			RespondError(c, http.StatusNotFound, err.Error())
		default:
			RespondError(c, http.StatusInternalServerError, err.Error())
		}
		return
	}

	service, err := failoverv2db.GetServiceByIDForUser(share.UserID, share.ServiceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 service")
		return
	}
	executions, err := failoverv2db.ListExecutionsByServiceForUser(share.UserID, share.ServiceID, 10)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 executions")
		return
	}

	accessPolicy, err := failoverv2svc.NormalizeShareAccessPolicy(share.AccessPolicy)
	if err != nil {
		accessPolicy = failoverv2svc.ShareAccessPolicyPublic
	}
	view := publicFailoverV2ShareView{
		Token:        strings.TrimSpace(share.ShareToken),
		Title:        strings.TrimSpace(share.Title),
		Note:         strings.TrimSpace(share.Note),
		AccessPolicy: accessPolicy,
		ExpiresAt:    share.ExpiresAt,
		CreatedAt:    share.CreatedAt,
		UpdatedAt:    share.UpdatedAt,
		GeneratedAt:  now,
		Service:      buildPublicFailoverV2ServiceView(service, executions),
	}

	consume := failoverv2svc.ShouldConsumeShare(share)
	recorded, err := failoverv2db.RecordShareAccess(share, consume, now)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "Failed to record failover v2 share access")
		return
	}
	if consume && !recorded {
		RespondError(c, http.StatusGone, failoverv2svc.ErrShareConsumed.Error())
		return
	}

	RespondSuccess(c, view)
}

func buildPublicFailoverV2ServiceView(service *models.FailoverV2Service, executions []models.FailoverV2Execution) publicFailoverV2ServiceView {
	members := make([]publicFailoverV2MemberView, 0, len(service.Members))
	enabledMemberCount := 0
	for _, member := range service.Members {
		if member.Enabled {
			enabledMemberCount++
		}
		members = append(members, publicFailoverV2MemberView{
			ID:                  member.ID,
			ServiceID:           member.ServiceID,
			Name:                strings.TrimSpace(member.Name),
			Enabled:             member.Enabled,
			Priority:            member.Priority,
			Mode:                strings.TrimSpace(member.Mode),
			DNSLines:            publicFailoverV2MemberDNSLines(member),
			CurrentAddress:      strings.TrimSpace(member.CurrentAddress),
			Provider:            strings.TrimSpace(member.Provider),
			TriggerFailureCount: member.TriggerFailureCount,
			LastExecutionID:     member.LastExecutionID,
			LastStatus:          strings.TrimSpace(member.LastStatus),
			LastMessage:         strings.TrimSpace(member.LastMessage),
			LastTriggeredAt:     member.LastTriggeredAt,
			LastSucceededAt:     member.LastSucceededAt,
			LastFailedAt:        member.LastFailedAt,
			CreatedAt:           member.CreatedAt,
			UpdatedAt:           member.UpdatedAt,
		})
	}

	recentExecutions := make([]publicFailoverV2ExecutionSummaryView, 0, len(executions))
	for _, execution := range executions {
		recentExecutions = append(recentExecutions, publicFailoverV2ExecutionSummaryView{
			ID:              execution.ID,
			ServiceID:       execution.ServiceID,
			MemberID:        execution.MemberID,
			Status:          strings.TrimSpace(execution.Status),
			TriggerReason:   strings.TrimSpace(execution.TriggerReason),
			DetachDNSStatus: strings.TrimSpace(execution.DetachDNSStatus),
			AttachDNSStatus: strings.TrimSpace(execution.AttachDNSStatus),
			CleanupStatus:   strings.TrimSpace(execution.CleanupStatus),
			ErrorMessage:    strings.TrimSpace(execution.ErrorMessage),
			StartedAt:       execution.StartedAt,
			FinishedAt:      execution.FinishedAt,
		})
	}

	return publicFailoverV2ServiceView{
		ID:                 service.ID,
		Name:               strings.TrimSpace(service.Name),
		Enabled:            service.Enabled,
		DNSProvider:        strings.TrimSpace(service.DNSProvider),
		LastExecutionID:    service.LastExecutionID,
		LastStatus:         strings.TrimSpace(service.LastStatus),
		LastMessage:        strings.TrimSpace(service.LastMessage),
		LastCheckedAt:      service.LastCheckedAt,
		MemberCount:        len(service.Members),
		EnabledMemberCount: enabledMemberCount,
		Members:            members,
		RecentExecutions:   recentExecutions,
		CreatedAt:          service.CreatedAt,
		UpdatedAt:          service.UpdatedAt,
	}
}

func publicFailoverV2MemberDNSLines(member models.FailoverV2Member) []string {
	lines := make([]string, 0, len(member.Lines))
	for _, line := range member.Lines {
		lineCode := strings.TrimSpace(line.LineCode)
		if lineCode != "" {
			lines = append(lines, lineCode)
		}
	}
	if len(lines) > 0 {
		return lines
	}
	lineCode := strings.TrimSpace(member.DNSLine)
	if lineCode == "" {
		return []string{}
	}
	return []string{lineCode}
}
