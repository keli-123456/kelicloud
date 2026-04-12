package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	clientdb "github.com/komari-monitor/komari/database/clients"
	clipboarddb "github.com/komari-monitor/komari/database/clipboard"
	failoverdb "github.com/komari-monitor/komari/database/failover"
	"github.com/komari-monitor/komari/database/models"
	failoversvc "github.com/komari-monitor/komari/utils/failover"
	"github.com/komari-monitor/komari/ws"
	"gorm.io/gorm"
)

type failoverPlanRequest struct {
	Name                string          `json:"name"`
	Priority            int             `json:"priority"`
	Enabled             *bool           `json:"enabled"`
	Provider            string          `json:"provider" binding:"required"`
	ProviderEntryID     string          `json:"provider_entry_id"`
	ProviderEntryGroup  string          `json:"provider_entry_group"`
	ActionType          string          `json:"action_type" binding:"required"`
	Payload             json.RawMessage `json:"payload"`
	AutoConnectGroup    string          `json:"auto_connect_group"`
	ScriptClipboardID   *int            `json:"script_clipboard_id"`
	ScriptClipboardIDs  []int           `json:"script_clipboard_ids"`
	ScriptTimeoutSec    int             `json:"script_timeout_sec"`
	WaitAgentTimeoutSec int             `json:"wait_agent_timeout_sec"`
}

type failoverTaskRequest struct {
	Name                          string                `json:"name" binding:"required"`
	Enabled                       *bool                 `json:"enabled"`
	CurrentClientUUID             string                `json:"current_client_uuid"`
	WatchClientUUID               string                `json:"watch_client_uuid"`
	FailureThreshold              int                   `json:"failure_threshold"`
	StaleAfterSeconds             int                   `json:"stale_after_seconds"`
	CooldownSeconds               int                   `json:"cooldown_seconds"`
	ProvisionRetryLimit           int                   `json:"provision_retry_limit"`
	ProvisionFailureFallbackLimit int                   `json:"provision_failure_fallback_limit"`
	DNSProvider                   string                `json:"dns_provider"`
	DNSEntryID                    string                `json:"dns_entry_id"`
	DNSPayload                    json.RawMessage       `json:"dns_payload"`
	DeleteStrategy                string                `json:"delete_strategy"`
	DeleteDelaySeconds            int                   `json:"delete_delay_seconds"`
	Plans                         []failoverPlanRequest `json:"plans" binding:"required"`
}

type failoverToggleRequest struct {
	Enabled bool `json:"enabled"`
}

type failoverPlanView struct {
	ID                  uint             `json:"id"`
	TaskID              uint             `json:"task_id"`
	Name                string           `json:"name"`
	Priority            int              `json:"priority"`
	Enabled             bool             `json:"enabled"`
	Provider            string           `json:"provider"`
	ProviderEntryID     string           `json:"provider_entry_id"`
	ProviderEntryGroup  string           `json:"provider_entry_group"`
	ActionType          string           `json:"action_type"`
	Payload             json.RawMessage  `json:"payload"`
	AutoConnectGroup    string           `json:"auto_connect_group"`
	ScriptClipboardID   *int             `json:"script_clipboard_id,omitempty"`
	ScriptClipboardIDs  []int            `json:"script_clipboard_ids,omitempty"`
	ScriptTimeoutSec    int              `json:"script_timeout_sec"`
	WaitAgentTimeoutSec int              `json:"wait_agent_timeout_sec"`
	CreatedAt           models.LocalTime `json:"created_at"`
	UpdatedAt           models.LocalTime `json:"updated_at"`
}

type failoverTaskView struct {
	ID                            uint                          `json:"id"`
	Name                          string                        `json:"name"`
	Enabled                       bool                          `json:"enabled"`
	CurrentClientUUID             string                        `json:"current_client_uuid"`
	CurrentAddress                string                        `json:"current_address"`
	CurrentInstanceRef            json.RawMessage               `json:"current_instance_ref"`
	TriggerFailureCount           int                           `json:"trigger_failure_count"`
	WatchClientUUID               string                        `json:"watch_client_uuid"`
	TriggerSource                 string                        `json:"trigger_source"`
	FailureThreshold              int                           `json:"failure_threshold"`
	StaleAfterSeconds             int                           `json:"stale_after_seconds"`
	CooldownSeconds               int                           `json:"cooldown_seconds"`
	ProvisionRetryLimit           int                           `json:"provision_retry_limit"`
	ProvisionFailureFallbackLimit int                           `json:"provision_failure_fallback_limit"`
	DNSProvider                   string                        `json:"dns_provider"`
	DNSEntryID                    string                        `json:"dns_entry_id"`
	DNSPayload                    json.RawMessage               `json:"dns_payload"`
	DeleteStrategy                string                        `json:"delete_strategy"`
	DeleteDelaySeconds            int                           `json:"delete_delay_seconds"`
	LastExecutionID               *uint                         `json:"last_execution_id,omitempty"`
	LastStatus                    string                        `json:"last_status"`
	LastMessage                   string                        `json:"last_message"`
	LastTriggeredAt               *models.LocalTime             `json:"last_triggered_at"`
	LastSucceededAt               *models.LocalTime             `json:"last_succeeded_at"`
	LastFailedAt                  *models.LocalTime             `json:"last_failed_at"`
	Probe                         failoverProbeView             `json:"probe"`
	CooldownRemaining             int64                         `json:"cooldown_remaining_seconds"`
	NextEligibleAt                *models.LocalTime             `json:"next_eligible_at"`
	NextScheduledAt               *models.LocalTime             `json:"next_scheduled_check_at"`
	NextScheduledIn               int64                         `json:"next_scheduled_check_remaining_seconds"`
	LatestExecution               *failoverExecutionSummaryView `json:"latest_execution,omitempty"`
	HasActiveExecution            bool                          `json:"has_active_execution"`
	Plans                         []failoverPlanView            `json:"plans"`
	CreatedAt                     models.LocalTime              `json:"created_at"`
	UpdatedAt                     models.LocalTime              `json:"updated_at"`
}

type failoverProbeView struct {
	Status              string            `json:"status"`
	Target              string            `json:"target,omitempty"`
	Latency             int64             `json:"latency,omitempty"`
	Message             string            `json:"message,omitempty"`
	CheckedAt           *models.LocalTime `json:"checked_at"`
	ReportUpdatedAt     *models.LocalTime `json:"report_updated_at"`
	ConsecutiveFailures int               `json:"consecutive_failures"`
	Stale               bool              `json:"stale"`
}

func planRequiresOldInstanceCleanup(plan models.FailoverPlan) bool {
	if !plan.Enabled {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(plan.Provider), "aws") {
		return true
	}
	return strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance
}

func normalizeFailoverDeleteStrategy(deleteStrategy string, plans []models.FailoverPlan) string {
	hasProvisionPlan := false
	for _, plan := range plans {
		if planRequiresOldInstanceCleanup(plan) {
			hasProvisionPlan = true
			break
		}
	}
	if !hasProvisionPlan {
		return models.FailoverDeleteStrategyKeep
	}

	normalized := strings.ToLower(strings.TrimSpace(deleteStrategy))
	switch normalized {
	case models.FailoverDeleteStrategyDeleteAfterSuccessDelay:
		return models.FailoverDeleteStrategyDeleteAfterSuccessDelay
	case models.FailoverDeleteStrategyDeleteAfterSuccess:
		return models.FailoverDeleteStrategyDeleteAfterSuccess
	default:
		return models.FailoverDeleteStrategyDeleteAfterSuccess
	}
}

type failoverExecutionSummaryView struct {
	ID                    uint                       `json:"id"`
	Status                string                     `json:"status"`
	TriggerReason         string                     `json:"trigger_reason"`
	SelectedPlanID        *uint                      `json:"selected_plan_id,omitempty"`
	AttemptedPlans        json.RawMessage            `json:"attempted_plans"`
	ScriptNameSnapshot    string                     `json:"script_name_snapshot"`
	ScriptStatus          string                     `json:"script_status"`
	ScriptExitCode        *int                       `json:"script_exit_code,omitempty"`
	ScriptOutputTruncated bool                       `json:"script_output_truncated"`
	DNSStatus             string                     `json:"dns_status"`
	DNSResult             json.RawMessage            `json:"dns_result"`
	CleanupStatus         string                     `json:"cleanup_status"`
	CleanupResult         json.RawMessage            `json:"cleanup_result"`
	LastStep              *failoverExecutionStepView `json:"last_step,omitempty"`
	ErrorMessage          string                     `json:"error_message"`
	StartedAt             models.LocalTime           `json:"started_at"`
	FinishedAt            *models.LocalTime          `json:"finished_at"`
}

type failoverExecutionStepView struct {
	ID          uint              `json:"id"`
	ExecutionID uint              `json:"execution_id"`
	Sort        int               `json:"sort"`
	StepKey     string            `json:"step_key"`
	StepLabel   string            `json:"step_label"`
	Status      string            `json:"status"`
	Message     string            `json:"message"`
	Detail      json.RawMessage   `json:"detail"`
	StartedAt   *models.LocalTime `json:"started_at"`
	FinishedAt  *models.LocalTime `json:"finished_at"`
	CreatedAt   models.LocalTime  `json:"created_at"`
	UpdatedAt   models.LocalTime  `json:"updated_at"`
}

type failoverExecutionActionView struct {
	Available bool   `json:"available"`
	Reason    string `json:"reason,omitempty"`
}

type failoverExecutionAvailableActionsView struct {
	RetryDNS     failoverExecutionActionView `json:"retry_dns"`
	RetryCleanup failoverExecutionActionView `json:"retry_cleanup"`
}

type failoverExecutionView struct {
	ID                    uint                                  `json:"id"`
	TaskID                uint                                  `json:"task_id"`
	Status                string                                `json:"status"`
	TriggerReason         string                                `json:"trigger_reason"`
	WatchClientUUID       string                                `json:"watch_client_uuid"`
	TriggerSnapshot       json.RawMessage                       `json:"trigger_snapshot"`
	SelectedPlanID        *uint                                 `json:"selected_plan_id,omitempty"`
	AttemptedPlans        json.RawMessage                       `json:"attempted_plans"`
	OldClientUUID         string                                `json:"old_client_uuid"`
	OldInstanceRef        json.RawMessage                       `json:"old_instance_ref"`
	OldAddresses          json.RawMessage                       `json:"old_addresses"`
	NewClientUUID         string                                `json:"new_client_uuid"`
	NewInstanceRef        json.RawMessage                       `json:"new_instance_ref"`
	NewAddresses          json.RawMessage                       `json:"new_addresses"`
	ScriptClipboardID     *int                                  `json:"script_clipboard_id,omitempty"`
	ScriptClipboardIDs    []int                                 `json:"script_clipboard_ids,omitempty"`
	ScriptNameSnapshot    string                                `json:"script_name_snapshot"`
	ScriptTaskID          string                                `json:"script_task_id"`
	ScriptStatus          string                                `json:"script_status"`
	ScriptExitCode        *int                                  `json:"script_exit_code,omitempty"`
	ScriptFinishedAt      *models.LocalTime                     `json:"script_finished_at"`
	ScriptOutput          string                                `json:"script_output"`
	ScriptOutputTruncated bool                                  `json:"script_output_truncated"`
	DNSProvider           string                                `json:"dns_provider"`
	DNSStatus             string                                `json:"dns_status"`
	DNSResult             json.RawMessage                       `json:"dns_result"`
	CleanupStatus         string                                `json:"cleanup_status"`
	CleanupResult         json.RawMessage                       `json:"cleanup_result"`
	AvailableActions      failoverExecutionAvailableActionsView `json:"available_actions"`
	ErrorMessage          string                                `json:"error_message"`
	StartedAt             models.LocalTime                      `json:"started_at"`
	FinishedAt            *models.LocalTime                     `json:"finished_at"`
	Steps                 []failoverExecutionStepView           `json:"steps,omitempty"`
	CreatedAt             models.LocalTime                      `json:"created_at"`
	UpdatedAt             models.LocalTime                      `json:"updated_at"`
}

func parseFailoverTaskID(c *gin.Context, param string) (uint, bool) {
	rawValue := strings.TrimSpace(c.Param(param))
	if rawValue == "" {
		api.RespondError(c, http.StatusBadRequest, "Task ID is required")
		return 0, false
	}

	parsed, err := strconv.ParseUint(rawValue, 10, 64)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid task ID")
		return 0, false
	}
	return uint(parsed), true
}

func parseFailoverExecutionID(c *gin.Context) (uint, bool) {
	rawValue := strings.TrimSpace(c.Param("id"))
	if rawValue == "" {
		api.RespondError(c, http.StatusBadRequest, "Execution ID is required")
		return 0, false
	}

	parsed, err := strconv.ParseUint(rawValue, 10, 64)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid execution ID")
		return 0, false
	}
	return uint(parsed), true
}

func normalizeJSONPayload(raw json.RawMessage, emptyValue string) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return emptyValue, nil
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, trimmed); err != nil {
		return "", err
	}
	return compact.String(), nil
}

func rawJSONOrNull(raw string) json.RawMessage {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return json.RawMessage("null")
	}
	if json.Valid([]byte(trimmed)) {
		return json.RawMessage(trimmed)
	}
	encoded, err := json.Marshal(trimmed)
	if err != nil {
		return json.RawMessage("null")
	}
	return json.RawMessage(encoded)
}

func normalizeRequestedFailoverScriptClipboardIDs(primary *int, values []int) []int {
	normalized := make([]int, 0, len(values)+1)
	seen := make(map[int]struct{}, len(values)+1)
	appendID := func(id int) {
		if id <= 0 {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
	}

	for _, id := range values {
		appendID(id)
	}
	if len(normalized) > 0 {
		return normalized
	}

	if primary != nil {
		appendID(*primary)
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func buildFailoverPlanView(plan models.FailoverPlan) failoverPlanView {
	scriptClipboardIDs := plan.EffectiveScriptClipboardIDs()
	return failoverPlanView{
		ID:                  plan.ID,
		TaskID:              plan.TaskID,
		Name:                plan.Name,
		Priority:            plan.Priority,
		Enabled:             plan.Enabled,
		Provider:            plan.Provider,
		ProviderEntryID:     plan.ProviderEntryID,
		ProviderEntryGroup:  plan.ProviderEntryGroup,
		ActionType:          plan.ActionType,
		Payload:             rawJSONOrNull(plan.Payload),
		AutoConnectGroup:    plan.AutoConnectGroup,
		ScriptClipboardID:   models.FirstFailoverScriptClipboardID(scriptClipboardIDs),
		ScriptClipboardIDs:  scriptClipboardIDs,
		ScriptTimeoutSec:    plan.ScriptTimeoutSec,
		WaitAgentTimeoutSec: plan.WaitAgentTimeoutSec,
		CreatedAt:           plan.CreatedAt,
		UpdatedAt:           plan.UpdatedAt,
	}
}

func buildFailoverExecutionAvailableActionsView(task *models.FailoverTask, execution *models.FailoverExecution) failoverExecutionAvailableActionsView {
	availableActions := failoversvc.DescribeExecutionAvailableActions(task, execution)
	return failoverExecutionAvailableActionsView{
		RetryDNS: failoverExecutionActionView{
			Available: availableActions.RetryDNS.Available,
			Reason:    strings.TrimSpace(availableActions.RetryDNS.Reason),
		},
		RetryCleanup: failoverExecutionActionView{
			Available: availableActions.RetryCleanup.Available,
			Reason:    strings.TrimSpace(availableActions.RetryCleanup.Reason),
		},
	}
}

func buildFailoverExecutionSummaryView(execution *models.FailoverExecution) *failoverExecutionSummaryView {
	if execution == nil {
		return nil
	}

	var lastStep *failoverExecutionStepView
	if len(execution.Steps) > 0 {
		last := execution.Steps[len(execution.Steps)-1]
		lastStep = &failoverExecutionStepView{
			ID:          last.ID,
			ExecutionID: last.ExecutionID,
			Sort:        last.Sort,
			StepKey:     last.StepKey,
			StepLabel:   last.StepLabel,
			Status:      last.Status,
			Message:     last.Message,
			Detail:      rawJSONOrNull(last.Detail),
			StartedAt:   last.StartedAt,
			FinishedAt:  last.FinishedAt,
			CreatedAt:   last.CreatedAt,
			UpdatedAt:   last.UpdatedAt,
		}
	}

	return &failoverExecutionSummaryView{
		ID:                    execution.ID,
		Status:                execution.Status,
		TriggerReason:         execution.TriggerReason,
		SelectedPlanID:        execution.SelectedPlanID,
		AttemptedPlans:        rawJSONOrNull(execution.AttemptedPlans),
		ScriptNameSnapshot:    execution.ScriptNameSnapshot,
		ScriptStatus:          execution.ScriptStatus,
		ScriptExitCode:        execution.ScriptExitCode,
		ScriptOutputTruncated: execution.ScriptOutputTruncated,
		DNSStatus:             execution.DNSStatus,
		DNSResult:             rawJSONOrNull(execution.DNSResult),
		CleanupStatus:         execution.CleanupStatus,
		CleanupResult:         rawJSONOrNull(execution.CleanupResult),
		LastStep:              lastStep,
		ErrorMessage:          execution.ErrorMessage,
		StartedAt:             execution.StartedAt,
		FinishedAt:            execution.FinishedAt,
	}
}

func buildFailoverProbeView(task *models.FailoverTask, report *common.Report, now time.Time) failoverProbeView {
	view := failoverProbeView{
		Status: "unavailable",
		Stale:  true,
	}
	if task != nil && strings.TrimSpace(task.WatchClientUUID) == "" {
		view.Message = "task is not initialized"
		return view
	}
	if task == nil || report == nil || report.CNConnectivity == nil {
		return view
	}

	view.Status = strings.TrimSpace(report.CNConnectivity.Status)
	view.Target = strings.TrimSpace(report.CNConnectivity.Target)
	view.Latency = report.CNConnectivity.Latency
	view.Message = strings.TrimSpace(report.CNConnectivity.Message)
	view.ConsecutiveFailures = report.CNConnectivity.ConsecutiveFailures

	if !report.CNConnectivity.CheckedAt.IsZero() {
		checkedAt := models.FromTime(report.CNConnectivity.CheckedAt)
		view.CheckedAt = &checkedAt
	}
	if !report.UpdatedAt.IsZero() {
		reportUpdatedAt := models.FromTime(report.UpdatedAt)
		view.ReportUpdatedAt = &reportUpdatedAt
	}

	reportTime := report.UpdatedAt
	if report.CNConnectivity.CheckedAt.After(reportTime) {
		reportTime = report.CNConnectivity.CheckedAt
	}
	view.Stale = reportTime.IsZero() || now.Sub(reportTime) > time.Duration(task.StaleAfterSeconds)*time.Second
	if view.Status == "" {
		view.Status = "unknown"
	}
	return view
}

func isFailoverExecutionActive(status string) bool {
	switch strings.TrimSpace(status) {
	case models.FailoverExecutionStatusQueued,
		models.FailoverExecutionStatusDetecting,
		models.FailoverExecutionStatusProvisioning,
		models.FailoverExecutionStatusRebindingIP,
		models.FailoverExecutionStatusWaitingAgent,
		models.FailoverExecutionStatusRunningScript,
		models.FailoverExecutionStatusSwitchingDNS,
		models.FailoverExecutionStatusCleaningOld:
		return true
	default:
		return false
	}
}

func buildFailoverTaskView(task *models.FailoverTask, latestExecution *models.FailoverExecution, probe failoverProbeView, now time.Time) failoverTaskView {
	plans := make([]failoverPlanView, 0, len(task.Plans))
	for _, plan := range task.Plans {
		plans = append(plans, buildFailoverPlanView(plan))
	}

	var cooldownRemaining int64
	var nextEligibleAt *models.LocalTime
	var nextScheduledAt *models.LocalTime
	var nextScheduledIn int64
	if task.CooldownSeconds > 0 && task.LastTriggeredAt != nil {
		next := task.LastTriggeredAt.ToTime().Add(time.Duration(task.CooldownSeconds) * time.Second)
		if next.After(now) {
			cooldownRemaining = int64(next.Sub(now).Seconds())
			if cooldownRemaining < 0 {
				cooldownRemaining = 0
			}
			value := models.FromTime(next)
			nextEligibleAt = &value
		}
	}

	hasActiveExecution := latestExecution != nil && isFailoverExecutionActive(latestExecution.Status)
	if task.Enabled && !hasActiveExecution {
		nextCheckTarget := now
		if nextEligibleAt != nil && nextEligibleAt.ToTime().After(nextCheckTarget) {
			nextCheckTarget = nextEligibleAt.ToTime()
		}
		if next, ok := failoversvc.NextScheduledRunAtOrAfter(nextCheckTarget); ok {
			if next.Before(now) {
				next = now
			}
			nextScheduledIn = int64(next.Sub(now).Seconds())
			if nextScheduledIn < 0 {
				nextScheduledIn = 0
			}
			value := models.FromTime(next)
			nextScheduledAt = &value
		}
	}

	return failoverTaskView{
		ID:                            task.ID,
		Name:                          task.Name,
		Enabled:                       task.Enabled,
		CurrentClientUUID:             task.WatchClientUUID,
		CurrentAddress:                task.CurrentAddress,
		CurrentInstanceRef:            rawJSONOrNull(task.CurrentInstanceRef),
		TriggerFailureCount:           task.TriggerFailureCount,
		WatchClientUUID:               task.WatchClientUUID,
		TriggerSource:                 task.TriggerSource,
		FailureThreshold:              task.FailureThreshold,
		StaleAfterSeconds:             task.StaleAfterSeconds,
		CooldownSeconds:               task.CooldownSeconds,
		ProvisionRetryLimit:           task.ProvisionRetryLimit,
		ProvisionFailureFallbackLimit: task.ProvisionFailureFallbackLimit,
		DNSProvider:                   task.DNSProvider,
		DNSEntryID:                    task.DNSEntryID,
		DNSPayload:                    rawJSONOrNull(task.DNSPayload),
		DeleteStrategy:                task.DeleteStrategy,
		DeleteDelaySeconds:            task.DeleteDelaySeconds,
		LastExecutionID:               task.LastExecutionID,
		LastStatus:                    task.LastStatus,
		LastMessage:                   task.LastMessage,
		LastTriggeredAt:               task.LastTriggeredAt,
		LastSucceededAt:               task.LastSucceededAt,
		LastFailedAt:                  task.LastFailedAt,
		Probe:                         probe,
		CooldownRemaining:             cooldownRemaining,
		NextEligibleAt:                nextEligibleAt,
		NextScheduledAt:               nextScheduledAt,
		NextScheduledIn:               nextScheduledIn,
		LatestExecution:               buildFailoverExecutionSummaryView(latestExecution),
		HasActiveExecution:            hasActiveExecution,
		Plans:                         plans,
		CreatedAt:                     task.CreatedAt,
		UpdatedAt:                     task.UpdatedAt,
	}
}

func buildFailoverExecutionView(execution *models.FailoverExecution, task *models.FailoverTask, includeSteps bool) failoverExecutionView {
	scriptClipboardIDs := execution.EffectiveScriptClipboardIDs()
	view := failoverExecutionView{
		ID:                    execution.ID,
		TaskID:                execution.TaskID,
		Status:                execution.Status,
		TriggerReason:         execution.TriggerReason,
		WatchClientUUID:       execution.WatchClientUUID,
		TriggerSnapshot:       rawJSONOrNull(execution.TriggerSnapshot),
		SelectedPlanID:        execution.SelectedPlanID,
		AttemptedPlans:        rawJSONOrNull(execution.AttemptedPlans),
		OldClientUUID:         execution.OldClientUUID,
		OldInstanceRef:        rawJSONOrNull(execution.OldInstanceRef),
		OldAddresses:          rawJSONOrNull(execution.OldAddresses),
		NewClientUUID:         execution.NewClientUUID,
		NewInstanceRef:        rawJSONOrNull(execution.NewInstanceRef),
		NewAddresses:          rawJSONOrNull(execution.NewAddresses),
		ScriptClipboardID:     models.FirstFailoverScriptClipboardID(scriptClipboardIDs),
		ScriptClipboardIDs:    scriptClipboardIDs,
		ScriptNameSnapshot:    execution.ScriptNameSnapshot,
		ScriptTaskID:          execution.ScriptTaskID,
		ScriptStatus:          execution.ScriptStatus,
		ScriptExitCode:        execution.ScriptExitCode,
		ScriptFinishedAt:      execution.ScriptFinishedAt,
		ScriptOutput:          execution.ScriptOutput,
		ScriptOutputTruncated: execution.ScriptOutputTruncated,
		DNSProvider:           execution.DNSProvider,
		DNSStatus:             execution.DNSStatus,
		DNSResult:             rawJSONOrNull(execution.DNSResult),
		CleanupStatus:         execution.CleanupStatus,
		CleanupResult:         rawJSONOrNull(execution.CleanupResult),
		AvailableActions:      buildFailoverExecutionAvailableActionsView(task, execution),
		ErrorMessage:          execution.ErrorMessage,
		StartedAt:             execution.StartedAt,
		FinishedAt:            execution.FinishedAt,
		CreatedAt:             execution.CreatedAt,
		UpdatedAt:             execution.UpdatedAt,
	}

	if includeSteps {
		view.Steps = make([]failoverExecutionStepView, 0, len(execution.Steps))
		for _, step := range execution.Steps {
			view.Steps = append(view.Steps, failoverExecutionStepView{
				ID:          step.ID,
				ExecutionID: step.ExecutionID,
				Sort:        step.Sort,
				StepKey:     step.StepKey,
				StepLabel:   step.StepLabel,
				Status:      step.Status,
				Message:     step.Message,
				Detail:      rawJSONOrNull(step.Detail),
				StartedAt:   step.StartedAt,
				FinishedAt:  step.FinishedAt,
				CreatedAt:   step.CreatedAt,
				UpdatedAt:   step.UpdatedAt,
			})
		}
	}

	return view
}

func validateCloudProviderEntryForScope(scope ownerScope, providerName, entryID string) error {
	providerName = strings.ToLower(strings.TrimSpace(providerName))
	entryID = strings.TrimSpace(entryID)
	if entryID == "" {
		switch providerName {
		case awsProviderName, digitalOceanProviderName, linodeProviderName:
			entryID = "active"
		default:
			return fmt.Errorf("provider entry id is required")
		}
	}

	switch providerName {
	case awsProviderName:
		_, addition, err := loadAWSAddition(scope, false)
		if err != nil {
			return err
		}
		if entryID == "active" && addition.ActiveCredential() != nil {
			return nil
		}
		if addition.FindCredential(entryID) != nil {
			return nil
		}
	case digitalOceanProviderName:
		_, addition, err := loadDigitalOceanAddition(scope, false)
		if err != nil {
			return err
		}
		if entryID == "active" && addition.ActiveToken() != nil {
			return nil
		}
		if addition.FindToken(entryID) != nil {
			return nil
		}
	case linodeProviderName:
		_, addition, err := loadLinodeAddition(scope, false)
		if err != nil {
			return err
		}
		if entryID == "active" && addition.ActiveToken() != nil {
			return nil
		}
		if addition.FindToken(entryID) != nil {
			return nil
		}
	case models.FailoverDNSProviderCloudflare, models.FailoverDNSProviderAliyun:
		if _, err := findDNSProviderEntryForScope(scope, providerName, entryID); err == nil {
			return nil
		} else {
			return err
		}
	default:
		return fmt.Errorf("unsupported provider: %s", providerName)
	}

	return fmt.Errorf("provider %s entry %s was not found", providerName, entryID)
}

func validateFailoverProviderSelectionForScope(scope ownerScope, providerName, entryID, entryGroup string) error {
	providerName = strings.ToLower(strings.TrimSpace(providerName))
	entryID = strings.TrimSpace(entryID)
	entryGroup = strings.TrimSpace(entryGroup)
	if entryID == "" {
		entryID = "active"
	}
	if err := validateFailoverProviderFeatureForScope(scope, providerName); err != nil {
		return err
	}

	switch providerName {
	case awsProviderName:
		_, addition, err := loadAWSAddition(scope, false)
		if err != nil {
			return err
		}
		if entryGroup != "" {
			if entryID != "active" {
				credential := addition.FindCredential(entryID)
				if credential == nil {
					return fmt.Errorf("provider %s entry %s was not found", providerName, entryID)
				}
				if strings.TrimSpace(credential.Group) != entryGroup {
					return fmt.Errorf("provider %s entry %s is not in group %s", providerName, entryID, entryGroup)
				}
				return nil
			}
			if active := addition.ActiveCredential(); active != nil && strings.TrimSpace(active.Group) == entryGroup {
				return nil
			}
			for _, credential := range addition.Credentials {
				if strings.TrimSpace(credential.Group) == entryGroup {
					return nil
				}
			}
			return fmt.Errorf("provider %s group %s was not found", providerName, entryGroup)
		}
		if entryID == "active" && addition.ActiveCredential() != nil {
			return nil
		}
		if addition.FindCredential(entryID) != nil {
			return nil
		}
	case digitalOceanProviderName:
		_, addition, err := loadDigitalOceanAddition(scope, false)
		if err != nil {
			return err
		}
		if entryGroup != "" {
			if entryID != "active" {
				token := addition.FindToken(entryID)
				if token == nil {
					return fmt.Errorf("provider %s entry %s was not found", providerName, entryID)
				}
				if strings.TrimSpace(token.Group) != entryGroup {
					return fmt.Errorf("provider %s entry %s is not in group %s", providerName, entryID, entryGroup)
				}
				return nil
			}
			if active := addition.ActiveToken(); active != nil && strings.TrimSpace(active.Group) == entryGroup {
				return nil
			}
			for _, token := range addition.Tokens {
				if strings.TrimSpace(token.Group) == entryGroup {
					return nil
				}
			}
			return fmt.Errorf("provider %s group %s was not found", providerName, entryGroup)
		}
		if entryID == "active" && addition.ActiveToken() != nil {
			return nil
		}
		if addition.FindToken(entryID) != nil {
			return nil
		}
	case linodeProviderName:
		_, addition, err := loadLinodeAddition(scope, false)
		if err != nil {
			return err
		}
		if entryGroup != "" {
			if entryID != "active" {
				token := addition.FindToken(entryID)
				if token == nil {
					return fmt.Errorf("provider %s entry %s was not found", providerName, entryID)
				}
				if strings.TrimSpace(token.Group) != entryGroup {
					return fmt.Errorf("provider %s entry %s is not in group %s", providerName, entryID, entryGroup)
				}
				return nil
			}
			if active := addition.ActiveToken(); active != nil && strings.TrimSpace(active.Group) == entryGroup {
				return nil
			}
			for _, token := range addition.Tokens {
				if strings.TrimSpace(token.Group) == entryGroup {
					return nil
				}
			}
			return fmt.Errorf("provider %s group %s was not found", providerName, entryGroup)
		}
		if entryID == "active" && addition.ActiveToken() != nil {
			return nil
		}
		if addition.FindToken(entryID) != nil {
			return nil
		}
	default:
		return fmt.Errorf("unsupported provider: %s", providerName)
	}

	return fmt.Errorf("provider %s entry %s was not found", providerName, entryID)
}

func validateFailoverProviderFeatureForScope(scope ownerScope, providerName string) error {
	requiredFeature := cloudProviderRequiredFeature(providerName)
	if requiredFeature == "" {
		return nil
	}
	if !scope.HasUser() {
		return fmt.Errorf("user context is required")
	}

	allowed, err := config.IsUserFeatureAllowed(scope.UserUUID, requiredFeature)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("cloud provider %s is disabled for this user", providerName)
	}
	return nil
}

func findDNSProviderEntryForScope(scope ownerScope, providerName, entryID string) (*cloudProviderEntry, error) {
	config, err := getCloudProviderConfigForScope(scope, providerName)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("provider %s is not configured", providerName)
		}
		return nil, err
	}

	trimmed := strings.TrimSpace(config.Addition)
	if trimmed == "" || trimmed == "{}" || trimmed == "null" {
		return nil, fmt.Errorf("provider %s is not configured", providerName)
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &object); err == nil {
		if rawEntries, ok := object["entries"]; ok {
			var entries []cloudProviderEntry
			if err := json.Unmarshal(rawEntries, &entries); err != nil {
				return nil, fmt.Errorf("invalid DNS provider configuration: %w", err)
			}
			for _, entry := range entries {
				normalized := normalizeCloudProviderEntry(entry)
				if normalized.ID == entryID {
					return &normalized, nil
				}
			}
			return nil, fmt.Errorf("provider %s entry %s was not found", providerName, entryID)
		}

		if entryID == "default" || entryID == "legacy-default" {
			values := map[string]interface{}{}
			if err := json.Unmarshal([]byte(trimmed), &values); err != nil {
				return nil, fmt.Errorf("invalid DNS provider configuration: %w", err)
			}
			entry := normalizeCloudProviderEntry(cloudProviderEntry{
				ID:     "default",
				Name:   defaultCloudProviderEntryName,
				Values: values,
			})
			return &entry, nil
		}
	}

	return nil, fmt.Errorf("provider %s entry %s was not found", providerName, entryID)
}

type failoverCloudflareDNSPayload struct {
	ZoneID     string `json:"zone_id,omitempty"`
	ZoneName   string `json:"zone_name,omitempty"`
	RecordName string `json:"record_name,omitempty"`
	RecordType string `json:"record_type,omitempty"`
	SyncIPv6   bool   `json:"sync_ipv6,omitempty"`
	TTL        int    `json:"ttl,omitempty"`
}

type failoverAliyunDNSPayload struct {
	DomainName string   `json:"domain_name,omitempty"`
	RR         string   `json:"rr,omitempty"`
	RecordType string   `json:"record_type,omitempty"`
	SyncIPv6   bool     `json:"sync_ipv6,omitempty"`
	TTL        int      `json:"ttl,omitempty"`
	Line       string   `json:"line,omitempty"`
	Lines      []string `json:"lines,omitempty"`
}

func normalizeAliyunRRInput(domainName, rr string) string {
	normalizedDomain := strings.Trim(strings.TrimSpace(domainName), ".")
	normalizedRR := strings.Trim(strings.TrimSpace(rr), ".")
	if normalizedRR == "" || normalizedRR == "@" {
		return "@"
	}
	if normalizedDomain == "" {
		return normalizedRR
	}
	if strings.EqualFold(normalizedRR, normalizedDomain) {
		return "@"
	}
	if len(normalizedRR) > len(normalizedDomain)+1 && normalizedRR[len(normalizedRR)-len(normalizedDomain)-1] == '.' && strings.EqualFold(normalizedRR[len(normalizedRR)-len(normalizedDomain):], normalizedDomain) {
		normalizedRR = strings.TrimSpace(normalizedRR[:len(normalizedRR)-len(normalizedDomain)-1])
		if normalizedRR == "" || normalizedRR == "@" {
			return "@"
		}
	}
	return normalizedRR
}

func validateAliyunRRInput(domainName, rr string) error {
	normalizedRR := normalizeAliyunRRInput(domainName, rr)
	if strings.Contains(normalizedRR, "://") {
		return fmt.Errorf("aliyun rr must be a host record like @, www, or api; do not enter a URL")
	}
	if strings.ContainsAny(normalizedRR, "/\\ \t\r\n") {
		return fmt.Errorf("aliyun rr must be a host record like @, www, or api; do not include spaces or path separators")
	}
	if strings.HasPrefix(normalizedRR, ".") || strings.HasSuffix(normalizedRR, ".") || strings.Contains(normalizedRR, "..") {
		return fmt.Errorf("aliyun rr is invalid; use only the host record such as @, www, or api")
	}
	return nil
}

func trimEntryValue(values map[string]interface{}, key string) string {
	if len(values) == 0 {
		return ""
	}
	if raw, ok := values[key]; ok {
		if value, ok := raw.(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func validateFailoverDNSPayload(scope ownerScope, providerName, entryID, payloadJSON string) error {
	entry, err := findDNSProviderEntryForScope(scope, providerName, entryID)
	if err != nil {
		return err
	}

	switch providerName {
	case models.FailoverDNSProviderCloudflare:
		var payload failoverCloudflareDNSPayload
		if strings.TrimSpace(payloadJSON) != "" {
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				return fmt.Errorf("cloudflare dns payload is invalid: %w", err)
			}
		}

		recordType := strings.ToUpper(strings.TrimSpace(payload.RecordType))
		if recordType == "" {
			recordType = "A"
		}
		switch recordType {
		case "A", "AAAA":
		default:
			return fmt.Errorf("unsupported dns record_type: %s", payload.RecordType)
		}

		if payload.TTL <= 0 {
			return fmt.Errorf("dns ttl must be greater than 0")
		}

		zoneID := firstNonEmpty(strings.TrimSpace(payload.ZoneID), trimEntryValue(entry.Values, "zone_id"))
		zoneName := firstNonEmpty(strings.TrimSpace(payload.ZoneName), trimEntryValue(entry.Values, "zone_name"))
		if zoneID == "" && zoneName == "" {
			return fmt.Errorf("cloudflare zone_name is required")
		}
		if strings.TrimSpace(payload.RecordName) == "" && zoneName == "" {
			return fmt.Errorf("cloudflare record_name requires zone_name when using the apex record")
		}
	case models.FailoverDNSProviderAliyun:
		var payload failoverAliyunDNSPayload
		if strings.TrimSpace(payloadJSON) != "" {
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				return fmt.Errorf("aliyun dns payload is invalid: %w", err)
			}
		}

		recordType := strings.ToUpper(strings.TrimSpace(payload.RecordType))
		if recordType == "" {
			recordType = "A"
		}
		switch recordType {
		case "A", "AAAA":
		default:
			return fmt.Errorf("unsupported dns record_type: %s", payload.RecordType)
		}

		if payload.TTL <= 0 {
			return fmt.Errorf("dns ttl must be greater than 0")
		}

		domainName := firstNonEmpty(strings.TrimSpace(payload.DomainName), trimEntryValue(entry.Values, "domain_name"))
		if domainName == "" {
			return fmt.Errorf("aliyun domain_name is required")
		}
		if err := validateAliyunRRInput(domainName, payload.RR); err != nil {
			return err
		}
		for _, line := range payload.Lines {
			if strings.TrimSpace(line) == "" {
				return fmt.Errorf("aliyun lines must not contain empty values")
			}
		}
	default:
		return fmt.Errorf("unsupported dns provider: %s", providerName)
	}

	return nil
}

func validateFailoverTaskRequest(scope ownerScope, req *failoverTaskRequest) (*models.FailoverTask, []models.FailoverPlan, error) {
	if req == nil {
		return nil, nil, fmt.Errorf("request is required")
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, nil, fmt.Errorf("name is required")
	}

	currentClientUUID := strings.TrimSpace(req.CurrentClientUUID)
	currentClientAddress := ""
	if currentClientUUID == "" {
		currentClientUUID = strings.TrimSpace(req.WatchClientUUID)
	}
	if currentClientUUID != "" {
		client, err := clientdb.GetClientByUUIDForUser(currentClientUUID, scope.UserUUID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, nil, fmt.Errorf("current client not found")
			}
			return nil, nil, err
		}
		currentClientAddress = strings.TrimSpace(firstNonEmpty(client.IPv4, client.IPv6))
	}

	dnsProvider := strings.ToLower(strings.TrimSpace(req.DNSProvider))
	dnsEntryID := strings.TrimSpace(req.DNSEntryID)
	dnsPayload := "{}"
	if dnsProvider != "" {
		switch dnsProvider {
		case models.FailoverDNSProviderCloudflare, models.FailoverDNSProviderAliyun:
		default:
			return nil, nil, fmt.Errorf("unsupported DNS provider: %s", req.DNSProvider)
		}

		if dnsEntryID == "" {
			return nil, nil, fmt.Errorf("dns_entry_id is required")
		}
		if err := validateCloudProviderEntryForScope(scope, dnsProvider, dnsEntryID); err != nil {
			return nil, nil, err
		}

		normalizedDNSPayload, err := normalizeJSONPayload(req.DNSPayload, "{}")
		if err != nil {
			return nil, nil, fmt.Errorf("invalid dns_payload: %w", err)
		}
		dnsPayload = normalizedDNSPayload
		if err := validateFailoverDNSPayload(scope, dnsProvider, dnsEntryID, dnsPayload); err != nil {
			return nil, nil, err
		}
	} else {
		dnsEntryID = ""
	}

	deleteStrategy := strings.ToLower(strings.TrimSpace(req.DeleteStrategy))
	if deleteStrategy == "" {
		deleteStrategy = models.FailoverDeleteStrategyKeep
	}
	switch deleteStrategy {
	case models.FailoverDeleteStrategyKeep,
		models.FailoverDeleteStrategyDeleteAfterSuccess,
		models.FailoverDeleteStrategyDeleteAfterSuccessDelay:
	default:
		return nil, nil, fmt.Errorf("unsupported delete strategy: %s", req.DeleteStrategy)
	}
	if req.DeleteDelaySeconds < 0 {
		return nil, nil, fmt.Errorf("delete_delay_seconds must be greater than or equal to 0")
	}

	if len(req.Plans) == 0 {
		return nil, nil, fmt.Errorf("at least one plan is required")
	}

	plans := make([]models.FailoverPlan, 0, len(req.Plans))
	for index, planReq := range req.Plans {
		provider := strings.ToLower(strings.TrimSpace(planReq.Provider))
		switch provider {
		case "aws", "digitalocean", "linode":
		default:
			return nil, nil, fmt.Errorf("unsupported plan provider: %s", planReq.Provider)
		}

		providerEntryID := strings.TrimSpace(planReq.ProviderEntryID)
		providerEntryGroup := strings.TrimSpace(planReq.ProviderEntryGroup)
		if providerEntryID == "" {
			providerEntryID = "active"
		}
		if err := validateFailoverProviderSelectionForScope(scope, provider, providerEntryID, providerEntryGroup); err != nil {
			return nil, nil, err
		}

		actionType := strings.ToLower(strings.TrimSpace(planReq.ActionType))
		switch actionType {
		case models.FailoverActionProvisionInstance, models.FailoverActionRebindPublicIP:
		default:
			return nil, nil, fmt.Errorf("unsupported plan action_type: %s", planReq.ActionType)
		}
		if actionType == models.FailoverActionRebindPublicIP && provider != "aws" {
			return nil, nil, fmt.Errorf("rebind_public_ip is currently only supported for aws")
		}

		payload, err := normalizeJSONPayload(planReq.Payload, "{}")
		if err != nil {
			return nil, nil, fmt.Errorf("invalid payload for plan %d: %w", index+1, err)
		}

		scriptClipboardIDs := normalizeRequestedFailoverScriptClipboardIDs(planReq.ScriptClipboardID, planReq.ScriptClipboardIDs)
		for _, scriptClipboardID := range scriptClipboardIDs {
			if _, err := clipboarddb.GetClipboardByIDForUser(scriptClipboardID, scope.UserUUID); err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil, fmt.Errorf("script clipboard %d was not found", scriptClipboardID)
				}
				return nil, nil, err
			}
		}

		enabled := true
		if planReq.Enabled != nil {
			enabled = *planReq.Enabled
		}

		planName := strings.TrimSpace(planReq.Name)
		if planName == "" {
			planName = fmt.Sprintf("Plan %d", index+1)
		}

		priority := planReq.Priority
		if priority <= 0 {
			priority = index + 1
		}

		scriptTimeout := planReq.ScriptTimeoutSec
		if scriptTimeout <= 0 {
			scriptTimeout = 600
		}

		waitAgentTimeout := planReq.WaitAgentTimeoutSec
		if waitAgentTimeout <= 0 {
			waitAgentTimeout = 600
		}

		primaryScriptClipboardID := models.FirstFailoverScriptClipboardID(scriptClipboardIDs)
		plans = append(plans, models.FailoverPlan{
			Name:                planName,
			Priority:            priority,
			Enabled:             enabled,
			Provider:            provider,
			ProviderEntryID:     providerEntryID,
			ProviderEntryGroup:  providerEntryGroup,
			ActionType:          actionType,
			Payload:             payload,
			AutoConnectGroup:    strings.TrimSpace(planReq.AutoConnectGroup),
			ScriptClipboardID:   primaryScriptClipboardID,
			ScriptClipboardIDs:  models.EncodeFailoverScriptClipboardIDs(scriptClipboardIDs),
			ScriptTimeoutSec:    scriptTimeout,
			WaitAgentTimeoutSec: waitAgentTimeout,
		})
	}

	deleteStrategy = normalizeFailoverDeleteStrategy(deleteStrategy, plans)
	deleteDelaySeconds := req.DeleteDelaySeconds
	if deleteStrategy != models.FailoverDeleteStrategyDeleteAfterSuccessDelay {
		deleteDelaySeconds = 0
	}

	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].Priority == plans[j].Priority {
			return i < j
		}
		return plans[i].Priority < plans[j].Priority
	})

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	failureThreshold := req.FailureThreshold
	if failureThreshold <= 0 {
		failureThreshold = 2
	}

	staleAfterSeconds := req.StaleAfterSeconds
	if staleAfterSeconds <= 0 {
		staleAfterSeconds = 300
	}

	cooldownSeconds := req.CooldownSeconds
	if cooldownSeconds < 0 {
		return nil, nil, fmt.Errorf("cooldown_seconds must be greater than or equal to 0")
	}
	provisionRetryLimit := req.ProvisionRetryLimit
	if provisionRetryLimit <= 0 {
		provisionRetryLimit = models.FailoverProvisionRetryLimitDefault
	}
	provisionFailureFallbackLimit := req.ProvisionFailureFallbackLimit
	if provisionFailureFallbackLimit <= 0 {
		provisionFailureFallbackLimit = models.FailoverProvisionFailureFallbackLimitDefault
	}

	return &models.FailoverTask{
		Name:                          name,
		Enabled:                       enabled,
		WatchClientUUID:               currentClientUUID,
		CurrentAddress:                currentClientAddress,
		TriggerSource:                 models.FailoverTriggerSourceCNConnectivity,
		FailureThreshold:              failureThreshold,
		StaleAfterSeconds:             staleAfterSeconds,
		CooldownSeconds:               cooldownSeconds,
		ProvisionRetryLimit:           provisionRetryLimit,
		ProvisionFailureFallbackLimit: provisionFailureFallbackLimit,
		DNSProvider:                   dnsProvider,
		DNSEntryID:                    dnsEntryID,
		DNSPayload:                    dnsPayload,
		DeleteStrategy:                deleteStrategy,
		DeleteDelaySeconds:            deleteDelaySeconds,
	}, plans, nil
}

func GetFailoverDNSCatalog(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	providerName := strings.TrimSpace(c.Query("provider"))
	entryID := strings.TrimSpace(c.Query("entry_id"))
	entryGroup := strings.TrimSpace(c.Query("entry_group"))
	if providerName == "" || (entryID == "" && entryGroup == "") {
		api.RespondError(c, http.StatusBadRequest, "provider and entry_id or entry_group are required")
		return
	}

	catalog, err := failoversvc.LoadDNSCatalog(
		scope.UserUUID,
		providerName,
		entryID,
		strings.TrimSpace(c.Query("zone_name")),
		strings.TrimSpace(c.Query("domain_name")),
	)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to load DNS catalog: "+err.Error())
		return
	}

	api.RespondSuccess(c, catalog)
}

func GetFailoverPlanCatalog(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	providerName := strings.TrimSpace(c.Query("provider"))
	entryID := strings.TrimSpace(c.Query("entry_id"))
	entryGroup := strings.TrimSpace(c.Query("entry_group"))
	if providerName == "" || (entryID == "" && entryGroup == "") {
		api.RespondError(c, http.StatusBadRequest, "provider and entry_id or entry_group are required")
		return
	}

	actionType := strings.TrimSpace(c.Query("action_type"))
	if actionType == "" {
		actionType = models.FailoverActionProvisionInstance
	}

	catalog, err := failoversvc.LoadPlanCatalog(
		scope.UserUUID,
		providerName,
		entryID,
		entryGroup,
		actionType,
		strings.TrimSpace(c.Query("service")),
		strings.TrimSpace(c.Query("region")),
		strings.TrimSpace(c.Query("mode")),
	)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to load plan catalog: "+err.Error())
		return
	}

	api.RespondSuccess(c, catalog)
}

func loadLatestExecutionLookup(taskList []models.FailoverTask) (map[uint]*models.FailoverExecution, error) {
	executionIDs := make([]uint, 0, len(taskList))
	for _, task := range taskList {
		if task.LastExecutionID != nil && *task.LastExecutionID > 0 {
			executionIDs = append(executionIDs, *task.LastExecutionID)
		}
	}

	executionByID, err := failoverdb.ListExecutionsByIDs(executionIDs)
	if err != nil {
		return nil, err
	}

	result := make(map[uint]*models.FailoverExecution, len(taskList))
	for _, task := range taskList {
		if task.LastExecutionID == nil || *task.LastExecutionID == 0 {
			continue
		}
		if execution := executionByID[*task.LastExecutionID]; execution != nil {
			result[task.ID] = execution
		}
	}
	return result, nil
}

func GetFailoverTasks(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	taskList, err := failoverdb.ListTasksByUser(scope.UserUUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list failover tasks: "+err.Error())
		return
	}

	now := time.Now()
	reports := ws.GetLatestReport()
	executionLookup, err := loadLatestExecutionLookup(taskList)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover execution summary: "+err.Error())
		return
	}

	response := make([]failoverTaskView, 0, len(taskList))
	for _, task := range taskList {
		taskCopy := task
		report := reports[strings.TrimSpace(taskCopy.WatchClientUUID)]
		response = append(response, buildFailoverTaskView(
			&taskCopy,
			executionLookup[taskCopy.ID],
			buildFailoverProbeView(&taskCopy, report, now),
			now,
		))
	}
	api.RespondSuccess(c, response)
}

func CreateFailoverTask(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var req failoverTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	task, plans, err := validateFailoverTaskRequest(scope, &req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	created, err := failoverdb.CreateTaskForUser(scope.UserUUID, task, plans)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to create failover task: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverTaskView(created, nil, failoverProbeView{Status: "unavailable", Stale: true}, time.Now()))
}

func PreviewFailoverTask(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var req failoverTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid failover request: "+err.Error())
		return
	}

	task, plans, err := validateFailoverTaskRequest(scope, &req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid failover request: "+err.Error())
		return
	}

	preview, err := failoversvc.PreviewTask(scope.UserUUID, *task, plans)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to preview failover task: "+err.Error())
		return
	}

	api.RespondSuccess(c, preview)
}

func GetFailoverTask(c *gin.Context) {
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
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover task: "+err.Error())
		return
	}

	var latestExecution *models.FailoverExecution
	if task.LastExecutionID != nil && *task.LastExecutionID > 0 {
		lookup, lookupErr := failoverdb.ListExecutionsByIDs([]uint{*task.LastExecutionID})
		if lookupErr != nil {
			api.RespondError(c, http.StatusInternalServerError, "Failed to load latest execution summary: "+lookupErr.Error())
			return
		}
		latestExecution = lookup[*task.LastExecutionID]
	}

	now := time.Now()
	reports := ws.GetLatestReport()
	report := reports[strings.TrimSpace(task.WatchClientUUID)]
	api.RespondSuccess(c, buildFailoverTaskView(task, latestExecution, buildFailoverProbeView(task, report, now), now))
}

func UpdateFailoverTask(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	taskID, ok := parseFailoverTaskID(c, "id")
	if !ok {
		return
	}

	var req failoverTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	task, plans, err := validateFailoverTaskRequest(scope, &req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	updated, err := failoverdb.UpdateTaskForUser(scope.UserUUID, taskID, task, plans)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to update failover task: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverTaskView(updated, nil, failoverProbeView{Status: "unavailable", Stale: true}, time.Now()))
}

func ToggleFailoverTask(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	taskID, ok := parseFailoverTaskID(c, "id")
	if !ok {
		return
	}

	var req failoverToggleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	task, err := failoverdb.ToggleTaskForUser(scope.UserUUID, taskID, req.Enabled)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to toggle failover task: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverTaskView(task, nil, failoverProbeView{Status: "unavailable", Stale: true}, time.Now()))
}

func DeleteFailoverTask(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	taskID, ok := parseFailoverTaskID(c, "id")
	if !ok {
		return
	}

	if err := failoverdb.DeleteTaskForUser(scope.UserUUID, taskID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete failover task: "+err.Error())
		return
	}

	api.RespondSuccess(c, nil)
}

func RunFailoverTask(c *gin.Context) {
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
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover task: "+err.Error())
		return
	}

	execution, err := failoversvc.RunTaskNowForUser(scope.UserUUID, taskID)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to start failover task: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverExecutionView(execution, task, false))
}

func StopFailoverExecution(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	executionID, ok := parseFailoverExecutionID(c)
	if !ok {
		return
	}

	execution, err := failoversvc.StopExecutionForUser(scope.UserUUID, executionID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover execution not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to stop failover execution: "+err.Error())
		return
	}

	task, taskErr := failoverdb.GetTaskByIDForUser(scope.UserUUID, execution.TaskID)
	if taskErr != nil {
		if errors.Is(taskErr, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover task: "+taskErr.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverExecutionView(execution, task, true))
}

func RetryFailoverExecutionDNS(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	executionID, ok := parseFailoverExecutionID(c)
	if !ok {
		return
	}

	execution, err := failoversvc.RetryDNSForUser(scope.UserUUID, executionID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover execution not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to retry failover DNS: "+err.Error())
		return
	}

	task, taskErr := failoverdb.GetTaskByIDForUser(scope.UserUUID, execution.TaskID)
	if taskErr != nil {
		if errors.Is(taskErr, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover task: "+taskErr.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverExecutionView(execution, task, true))
}

func RetryFailoverExecutionCleanup(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	executionID, ok := parseFailoverExecutionID(c)
	if !ok {
		return
	}

	execution, err := failoversvc.RetryCleanupForUser(scope.UserUUID, executionID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover execution not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to retry failover cleanup: "+err.Error())
		return
	}

	task, taskErr := failoverdb.GetTaskByIDForUser(scope.UserUUID, execution.TaskID)
	if taskErr != nil {
		if errors.Is(taskErr, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover task: "+taskErr.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverExecutionView(execution, task, true))
}

func GetFailoverExecutions(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	taskID, ok := parseFailoverTaskID(c, "id")
	if !ok {
		return
	}

	task, taskErr := failoverdb.GetTaskByIDForUser(scope.UserUUID, taskID)
	if taskErr != nil {
		if errors.Is(taskErr, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover task: "+taskErr.Error())
		return
	}

	limit := 20
	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			api.RespondError(c, http.StatusBadRequest, "Invalid limit")
			return
		}
		limit = parsed
	}

	executions, err := failoverdb.ListExecutionsByTaskForUser(scope.UserUUID, taskID, limit)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to list failover executions: "+err.Error())
		return
	}

	response := make([]failoverExecutionView, 0, len(executions))
	for _, execution := range executions {
		executionCopy := execution
		response = append(response, buildFailoverExecutionView(&executionCopy, task, false))
	}
	api.RespondSuccess(c, response)
}

func GetFailoverExecution(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	executionID, ok := parseFailoverExecutionID(c)
	if !ok {
		return
	}

	execution, err := failoverdb.GetExecutionByIDForUser(scope.UserUUID, executionID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover execution not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover execution: "+err.Error())
		return
	}

	task, taskErr := failoverdb.GetTaskByIDForUser(scope.UserUUID, execution.TaskID)
	if taskErr != nil {
		if errors.Is(taskErr, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover task not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover task: "+taskErr.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverExecutionView(execution, task, true))
}
