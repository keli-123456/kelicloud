package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	failoverdb "github.com/komari-monitor/komari/database/failover"
	"github.com/komari-monitor/komari/database/models"
	failoversvc "github.com/komari-monitor/komari/utils/failover"
	"gorm.io/gorm"
)

type publicFailoverShareView struct {
	Token        string                 `json:"token"`
	Title        string                 `json:"title,omitempty"`
	Note         string                 `json:"note,omitempty"`
	AccessPolicy string                 `json:"access_policy"`
	ExpiresAt    *time.Time             `json:"expires_at,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	GeneratedAt  time.Time              `json:"generated_at"`
	Task         publicFailoverTaskView `json:"task"`
}

type publicFailoverTaskView struct {
	ID                            uint                                 `json:"id"`
	Name                          string                               `json:"name"`
	Enabled                       bool                                 `json:"enabled"`
	CurrentAddress                string                               `json:"current_address"`
	TriggerFailureCount           int                                  `json:"trigger_failure_count"`
	TriggerSource                 string                               `json:"trigger_source"`
	FailureThreshold              int                                  `json:"failure_threshold"`
	StaleAfterSeconds             int                                  `json:"stale_after_seconds"`
	CooldownSeconds               int                                  `json:"cooldown_seconds"`
	ProvisionRetryLimit           int                                  `json:"provision_retry_limit"`
	ProvisionFailureFallbackLimit int                                  `json:"provision_failure_fallback_limit"`
	DNSProvider                   string                               `json:"dns_provider"`
	DNSEntryID                    string                               `json:"dns_entry_id"`
	DeleteStrategy                string                               `json:"delete_strategy"`
	DeleteDelaySeconds            int                                  `json:"delete_delay_seconds"`
	LastExecutionID               *uint                                `json:"last_execution_id,omitempty"`
	LastStatus                    string                               `json:"last_status"`
	LastMessage                   string                               `json:"last_message"`
	LastTriggeredAt               *models.LocalTime                    `json:"last_triggered_at"`
	LastSucceededAt               *models.LocalTime                    `json:"last_succeeded_at"`
	LastFailedAt                  *models.LocalTime                    `json:"last_failed_at"`
	Plans                         []publicFailoverPlanView             `json:"plans"`
	LatestExecution               *publicFailoverExecutionSummaryView  `json:"latest_execution,omitempty"`
	RecentExecutions              []publicFailoverExecutionSummaryView `json:"recent_executions"`
	CreatedAt                     models.LocalTime                     `json:"created_at"`
	UpdatedAt                     models.LocalTime                     `json:"updated_at"`
}

type publicFailoverPlanView struct {
	ID                  uint             `json:"id"`
	TaskID              uint             `json:"task_id"`
	Name                string           `json:"name"`
	Priority            int              `json:"priority"`
	Enabled             bool             `json:"enabled"`
	Provider            string           `json:"provider"`
	ProviderEntryGroup  string           `json:"provider_entry_group"`
	ActionType          string           `json:"action_type"`
	AutoConnectGroup    string           `json:"auto_connect_group"`
	ScriptTimeoutSec    int              `json:"script_timeout_sec"`
	WaitAgentTimeoutSec int              `json:"wait_agent_timeout_sec"`
	CreatedAt           models.LocalTime `json:"created_at"`
	UpdatedAt           models.LocalTime `json:"updated_at"`
}

type publicFailoverExecutionSummaryView struct {
	ID                    uint              `json:"id"`
	Status                string            `json:"status"`
	TriggerReason         string            `json:"trigger_reason"`
	SelectedPlanID        *uint             `json:"selected_plan_id,omitempty"`
	ScriptNameSnapshot    string            `json:"script_name_snapshot"`
	ScriptStatus          string            `json:"script_status"`
	ScriptExitCode        *int              `json:"script_exit_code,omitempty"`
	ScriptOutputTruncated bool              `json:"script_output_truncated"`
	DNSStatus             string            `json:"dns_status"`
	CleanupStatus         string            `json:"cleanup_status"`
	ErrorMessage          string            `json:"error_message"`
	StartedAt             models.LocalTime  `json:"started_at"`
	FinishedAt            *models.LocalTime `json:"finished_at"`
}

func GetPublicFailoverShare(c *gin.Context) {
	token := strings.TrimSpace(c.Param("token"))
	if token == "" {
		RespondError(c, http.StatusBadRequest, "Invalid failover share token")
		return
	}

	share, err := failoverdb.GetShareByToken(token)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			RespondError(c, http.StatusNotFound, "Failover share not found")
			return
		}
		RespondError(c, http.StatusInternalServerError, "Failed to load failover share")
		return
	}

	now := time.Now().UTC()
	if err := failoversvc.ValidateSharePublicAccess(share, now); err != nil {
		switch {
		case errors.Is(err, failoversvc.ErrShareExpired), errors.Is(err, failoversvc.ErrShareConsumed):
			RespondError(c, http.StatusGone, err.Error())
		case errors.Is(err, failoversvc.ErrShareNotFound):
			RespondError(c, http.StatusNotFound, err.Error())
		default:
			RespondError(c, http.StatusInternalServerError, err.Error())
		}
		return
	}

	task, err := failoverdb.GetTaskByIDForUser(share.UserID, share.TaskID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		RespondError(c, http.StatusInternalServerError, "Failed to load failover task")
		return
	}
	executions, err := failoverdb.ListExecutionsByTaskForUser(share.UserID, share.TaskID, 10)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "Failed to load failover executions")
		return
	}

	accessPolicy, err := failoversvc.NormalizeShareAccessPolicy(share.AccessPolicy)
	if err != nil {
		accessPolicy = failoversvc.ShareAccessPolicyPublic
	}
	view := publicFailoverShareView{
		Token:        strings.TrimSpace(share.ShareToken),
		Title:        strings.TrimSpace(share.Title),
		Note:         strings.TrimSpace(share.Note),
		AccessPolicy: accessPolicy,
		ExpiresAt:    share.ExpiresAt,
		CreatedAt:    share.CreatedAt,
		UpdatedAt:    share.UpdatedAt,
		GeneratedAt:  now,
		Task:         buildPublicFailoverTaskView(task, executions),
	}

	consume := failoversvc.ShouldConsumeShare(share)
	recorded, err := failoverdb.RecordShareAccess(share, consume, now)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "Failed to record failover share access")
		return
	}
	if consume && !recorded {
		RespondError(c, http.StatusGone, failoversvc.ErrShareConsumed.Error())
		return
	}

	RespondSuccess(c, view)
}

func buildPublicFailoverTaskView(task *models.FailoverTask, executions []models.FailoverExecution) publicFailoverTaskView {
	plans := make([]publicFailoverPlanView, 0, len(task.Plans))
	for _, plan := range task.Plans {
		plans = append(plans, publicFailoverPlanView{
			ID:                  plan.ID,
			TaskID:              plan.TaskID,
			Name:                strings.TrimSpace(plan.Name),
			Priority:            plan.Priority,
			Enabled:             plan.Enabled,
			Provider:            strings.TrimSpace(plan.Provider),
			ProviderEntryGroup:  strings.TrimSpace(plan.ProviderEntryGroup),
			ActionType:          strings.TrimSpace(plan.ActionType),
			AutoConnectGroup:    strings.TrimSpace(plan.AutoConnectGroup),
			ScriptTimeoutSec:    plan.ScriptTimeoutSec,
			WaitAgentTimeoutSec: plan.WaitAgentTimeoutSec,
			CreatedAt:           plan.CreatedAt,
			UpdatedAt:           plan.UpdatedAt,
		})
	}

	recentExecutions := make([]publicFailoverExecutionSummaryView, 0, len(executions))
	for _, execution := range executions {
		recentExecutions = append(recentExecutions, buildPublicFailoverExecutionSummaryView(execution))
	}

	var latestExecution *publicFailoverExecutionSummaryView
	if len(recentExecutions) > 0 {
		latestExecution = &recentExecutions[0]
	}

	return publicFailoverTaskView{
		ID:                            task.ID,
		Name:                          strings.TrimSpace(task.Name),
		Enabled:                       task.Enabled,
		CurrentAddress:                strings.TrimSpace(task.CurrentAddress),
		TriggerFailureCount:           task.TriggerFailureCount,
		TriggerSource:                 strings.TrimSpace(task.TriggerSource),
		FailureThreshold:              task.FailureThreshold,
		StaleAfterSeconds:             task.StaleAfterSeconds,
		CooldownSeconds:               task.CooldownSeconds,
		ProvisionRetryLimit:           task.ProvisionRetryLimit,
		ProvisionFailureFallbackLimit: task.ProvisionFailureFallbackLimit,
		DNSProvider:                   strings.TrimSpace(task.DNSProvider),
		DNSEntryID:                    strings.TrimSpace(task.DNSEntryID),
		DeleteStrategy:                strings.TrimSpace(task.DeleteStrategy),
		DeleteDelaySeconds:            task.DeleteDelaySeconds,
		LastExecutionID:               task.LastExecutionID,
		LastStatus:                    strings.TrimSpace(task.LastStatus),
		LastMessage:                   strings.TrimSpace(task.LastMessage),
		LastTriggeredAt:               task.LastTriggeredAt,
		LastSucceededAt:               task.LastSucceededAt,
		LastFailedAt:                  task.LastFailedAt,
		Plans:                         plans,
		LatestExecution:               latestExecution,
		RecentExecutions:              recentExecutions,
		CreatedAt:                     task.CreatedAt,
		UpdatedAt:                     task.UpdatedAt,
	}
}

func buildPublicFailoverExecutionSummaryView(execution models.FailoverExecution) publicFailoverExecutionSummaryView {
	return publicFailoverExecutionSummaryView{
		ID:                    execution.ID,
		Status:                strings.TrimSpace(execution.Status),
		TriggerReason:         strings.TrimSpace(execution.TriggerReason),
		SelectedPlanID:        execution.SelectedPlanID,
		ScriptNameSnapshot:    strings.TrimSpace(execution.ScriptNameSnapshot),
		ScriptStatus:          strings.TrimSpace(execution.ScriptStatus),
		ScriptExitCode:        execution.ScriptExitCode,
		ScriptOutputTruncated: execution.ScriptOutputTruncated,
		DNSStatus:             strings.TrimSpace(execution.DNSStatus),
		CleanupStatus:         strings.TrimSpace(execution.CleanupStatus),
		ErrorMessage:          strings.TrimSpace(execution.ErrorMessage),
		StartedAt:             execution.StartedAt,
		FinishedAt:            execution.FinishedAt,
	}
}
