package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	clientdb "github.com/komari-monitor/komari/database/clients"
	clipboarddb "github.com/komari-monitor/komari/database/clipboard"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	failoverv2svc "github.com/komari-monitor/komari/utils/failoverv2"
	"gorm.io/gorm"
)

type failoverV2ServiceRequest struct {
	Name                string          `json:"name" binding:"required"`
	Enabled             *bool           `json:"enabled"`
	DNSProvider         string          `json:"dns_provider" binding:"required"`
	DNSEntryID          string          `json:"dns_entry_id" binding:"required"`
	DNSPayload          json.RawMessage `json:"dns_payload"`
	ScriptClipboardIDs  []int           `json:"script_clipboard_ids"`
	ScriptTimeoutSec    int             `json:"script_timeout_sec"`
	WaitAgentTimeoutSec int             `json:"wait_agent_timeout_sec"`
	DeleteStrategy      string          `json:"delete_strategy"`
	DeleteDelaySeconds  int             `json:"delete_delay_seconds"`
}

type failoverV2MemberRequest struct {
	Name               string          `json:"name"`
	Enabled            *bool           `json:"enabled"`
	Priority           int             `json:"priority"`
	Mode               string          `json:"mode"`
	WatchClientUUID    string          `json:"watch_client_uuid"`
	DNSLines           []string        `json:"dns_lines"`
	DNSLine            string          `json:"dns_line"`
	DNSRecordRefs      json.RawMessage `json:"dns_record_refs"`
	CurrentAddress     string          `json:"current_address"`
	CurrentInstanceRef json.RawMessage `json:"current_instance_ref"`
	Provider           string          `json:"provider"`
	ProviderEntryID    string          `json:"provider_entry_id"`
	ProviderEntryGroup string          `json:"provider_entry_group"`
	PlanPayload        json.RawMessage `json:"plan_payload"`
	FailureThreshold   int             `json:"failure_threshold"`
	StaleAfterSeconds  int             `json:"stale_after_seconds"`
	CooldownSeconds    int             `json:"cooldown_seconds"`
}

type failoverV2EnabledRequest struct {
	Enabled *bool `json:"enabled"`
}

type failoverV2ValidationCheckView struct {
	Key     string `json:"key"`
	Label   string `json:"label"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type failoverV2ValidationView struct {
	OK     bool                            `json:"ok"`
	Checks []failoverV2ValidationCheckView `json:"checks"`
}

type failoverV2ServiceValidationView struct {
	ServiceID   uint                            `json:"service_id"`
	ServiceName string                          `json:"service_name"`
	Enabled     bool                            `json:"enabled"`
	OK          bool                            `json:"ok"`
	Checks      []failoverV2ValidationCheckView `json:"checks"`
}

type failoverV2BulkValidationView struct {
	OK       bool                              `json:"ok"`
	Checked  int                               `json:"checked"`
	Failed   int                               `json:"failed"`
	Warnings int                               `json:"warnings"`
	Services []failoverV2ServiceValidationView `json:"services"`
}

type failoverV2MemberLineView struct {
	LineCode      string          `json:"line_code"`
	DNSRecordRefs json.RawMessage `json:"dns_record_refs"`
}

type failoverV2MemberView struct {
	ID                  uint                       `json:"id"`
	ServiceID           uint                       `json:"service_id"`
	Name                string                     `json:"name"`
	Enabled             bool                       `json:"enabled"`
	Priority            int                        `json:"priority"`
	Mode                string                     `json:"mode"`
	WatchClientUUID     string                     `json:"watch_client_uuid"`
	DNSLines            []string                   `json:"dns_lines"`
	Lines               []failoverV2MemberLineView `json:"lines,omitempty"`
	DNSLine             string                     `json:"dns_line"`
	DNSRecordRefs       json.RawMessage            `json:"dns_record_refs"`
	CurrentAddress      string                     `json:"current_address"`
	CurrentInstanceRef  json.RawMessage            `json:"current_instance_ref"`
	Provider            string                     `json:"provider"`
	ProviderEntryID     string                     `json:"provider_entry_id"`
	ProviderEntryGroup  string                     `json:"provider_entry_group"`
	PlanPayload         json.RawMessage            `json:"plan_payload"`
	FailureThreshold    int                        `json:"failure_threshold"`
	StaleAfterSeconds   int                        `json:"stale_after_seconds"`
	CooldownSeconds     int                        `json:"cooldown_seconds"`
	TriggerFailureCount int                        `json:"trigger_failure_count"`
	LastExecutionID     *uint                      `json:"last_execution_id,omitempty"`
	LastStatus          string                     `json:"last_status"`
	LastMessage         string                     `json:"last_message"`
	LastTriggeredAt     *models.LocalTime          `json:"last_triggered_at"`
	LastSucceededAt     *models.LocalTime          `json:"last_succeeded_at"`
	LastFailedAt        *models.LocalTime          `json:"last_failed_at"`
	CreatedAt           models.LocalTime           `json:"created_at"`
	UpdatedAt           models.LocalTime           `json:"updated_at"`
}

type failoverV2ExecutionSummaryView struct {
	ID              uint              `json:"id"`
	ServiceID       uint              `json:"service_id"`
	MemberID        uint              `json:"member_id"`
	Status          string            `json:"status"`
	TriggerReason   string            `json:"trigger_reason"`
	TriggerSnapshot json.RawMessage   `json:"trigger_snapshot"`
	OldClientUUID   string            `json:"old_client_uuid"`
	OldInstanceRef  json.RawMessage   `json:"old_instance_ref"`
	OldAddresses    json.RawMessage   `json:"old_addresses"`
	DetachDNSStatus string            `json:"detach_dns_status"`
	DetachDNSResult json.RawMessage   `json:"detach_dns_result"`
	NewClientUUID   string            `json:"new_client_uuid"`
	NewInstanceRef  json.RawMessage   `json:"new_instance_ref"`
	NewAddresses    json.RawMessage   `json:"new_addresses"`
	AttachDNSStatus string            `json:"attach_dns_status"`
	AttachDNSResult json.RawMessage   `json:"attach_dns_result"`
	CleanupStatus   string            `json:"cleanup_status"`
	CleanupResult   json.RawMessage   `json:"cleanup_result"`
	ErrorMessage    string            `json:"error_message"`
	StartedAt       models.LocalTime  `json:"started_at"`
	FinishedAt      *models.LocalTime `json:"finished_at"`
}

type failoverV2ExecutionStepView struct {
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

type failoverV2ExecutionView struct {
	ID               uint                                    `json:"id"`
	ServiceID        uint                                    `json:"service_id"`
	MemberID         uint                                    `json:"member_id"`
	Status           string                                  `json:"status"`
	TriggerReason    string                                  `json:"trigger_reason"`
	TriggerSnapshot  json.RawMessage                         `json:"trigger_snapshot"`
	OldClientUUID    string                                  `json:"old_client_uuid"`
	OldInstanceRef   json.RawMessage                         `json:"old_instance_ref"`
	OldAddresses     json.RawMessage                         `json:"old_addresses"`
	DetachDNSStatus  string                                  `json:"detach_dns_status"`
	DetachDNSResult  json.RawMessage                         `json:"detach_dns_result"`
	NewClientUUID    string                                  `json:"new_client_uuid"`
	NewInstanceRef   json.RawMessage                         `json:"new_instance_ref"`
	NewAddresses     json.RawMessage                         `json:"new_addresses"`
	AttachDNSStatus  string                                  `json:"attach_dns_status"`
	AttachDNSResult  json.RawMessage                         `json:"attach_dns_result"`
	CleanupStatus    string                                  `json:"cleanup_status"`
	CleanupResult    json.RawMessage                         `json:"cleanup_result"`
	ErrorMessage     string                                  `json:"error_message"`
	AvailableActions failoverV2ExecutionAvailableActionsView `json:"available_actions"`
	StartedAt        models.LocalTime                        `json:"started_at"`
	FinishedAt       *models.LocalTime                       `json:"finished_at"`
	Steps            []failoverV2ExecutionStepView           `json:"steps,omitempty"`
	CreatedAt        models.LocalTime                        `json:"created_at"`
	UpdatedAt        models.LocalTime                        `json:"updated_at"`
}

type failoverV2ExecutionActionView struct {
	Available bool   `json:"available"`
	Reason    string `json:"reason,omitempty"`
}

type failoverV2ExecutionAvailableActionsView struct {
	StopExecution  failoverV2ExecutionActionView `json:"stop"`
	RetryAttachDNS failoverV2ExecutionActionView `json:"retry_attach_dns"`
	RetryCleanup   failoverV2ExecutionActionView `json:"retry_cleanup"`
}

type failoverV2PendingCleanupActionView struct {
	Available bool   `json:"available"`
	Reason    string `json:"reason,omitempty"`
}

type failoverV2PendingCleanupAvailableActionsView struct {
	Retry            failoverV2PendingCleanupActionView `json:"retry"`
	MarkResolved     failoverV2PendingCleanupActionView `json:"mark_resolved"`
	MarkManualReview failoverV2PendingCleanupActionView `json:"mark_manual_review"`
}

type failoverV2PendingCleanupView struct {
	ID               uint                                         `json:"id"`
	ServiceID        uint                                         `json:"service_id"`
	MemberID         uint                                         `json:"member_id"`
	ExecutionID      uint                                         `json:"execution_id"`
	Provider         string                                       `json:"provider"`
	ProviderEntryID  string                                       `json:"provider_entry_id"`
	ResourceType     string                                       `json:"resource_type"`
	ResourceID       string                                       `json:"resource_id"`
	InstanceRef      json.RawMessage                              `json:"instance_ref"`
	CleanupLabel     string                                       `json:"cleanup_label"`
	Status           string                                       `json:"status"`
	AttemptCount     int                                          `json:"attempt_count"`
	LastError        string                                       `json:"last_error"`
	LastAttemptedAt  *models.LocalTime                            `json:"last_attempted_at"`
	NextRetryAt      *models.LocalTime                            `json:"next_retry_at"`
	ResolvedAt       *models.LocalTime                            `json:"resolved_at"`
	AvailableActions failoverV2PendingCleanupAvailableActionsView `json:"available_actions"`
	CreatedAt        models.LocalTime                             `json:"created_at"`
	UpdatedAt        models.LocalTime                             `json:"updated_at"`
}

type failoverV2ServiceView struct {
	ID                  uint                             `json:"id"`
	Name                string                           `json:"name"`
	Enabled             bool                             `json:"enabled"`
	DNSProvider         string                           `json:"dns_provider"`
	DNSEntryID          string                           `json:"dns_entry_id"`
	DNSPayload          json.RawMessage                  `json:"dns_payload"`
	ScriptClipboardIDs  json.RawMessage                  `json:"script_clipboard_ids"`
	ScriptTimeoutSec    int                              `json:"script_timeout_sec"`
	WaitAgentTimeoutSec int                              `json:"wait_agent_timeout_sec"`
	DeleteStrategy      string                           `json:"delete_strategy"`
	DeleteDelaySeconds  int                              `json:"delete_delay_seconds"`
	LastExecutionID     *uint                            `json:"last_execution_id,omitempty"`
	LastStatus          string                           `json:"last_status"`
	LastMessage         string                           `json:"last_message"`
	MemberCount         int                              `json:"member_count"`
	EnabledMemberCount  int                              `json:"enabled_member_count"`
	Members             []failoverV2MemberView           `json:"members,omitempty"`
	RecentExecutions    []failoverV2ExecutionSummaryView `json:"recent_executions,omitempty"`
	CreatedAt           models.LocalTime                 `json:"created_at"`
	UpdatedAt           models.LocalTime                 `json:"updated_at"`
}

func parseFailoverV2ServiceID(c *gin.Context) (uint, bool) {
	rawValue := strings.TrimSpace(c.Param("id"))
	if rawValue == "" {
		api.RespondError(c, http.StatusBadRequest, "Service ID is required")
		return 0, false
	}

	parsed, err := strconv.ParseUint(rawValue, 10, 64)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid service ID")
		return 0, false
	}
	return uint(parsed), true
}

func parseOptionalFailoverV2ServiceID(c *gin.Context) (uint, bool) {
	rawValue := strings.TrimSpace(c.Param("id"))
	if rawValue == "" {
		return 0, true
	}

	parsed, err := strconv.ParseUint(rawValue, 10, 64)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid service ID")
		return 0, false
	}
	return uint(parsed), true
}

func parseOptionalFailoverV2MemberID(c *gin.Context) (uint, bool) {
	rawValue := strings.TrimSpace(c.Param("member_id"))
	if rawValue == "" {
		return 0, true
	}

	parsed, err := strconv.ParseUint(rawValue, 10, 64)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid member ID")
		return 0, false
	}
	return uint(parsed), true
}

func parseFailoverV2MemberID(c *gin.Context) (uint, bool) {
	rawValue := strings.TrimSpace(c.Param("member_id"))
	if rawValue == "" {
		api.RespondError(c, http.StatusBadRequest, "Member ID is required")
		return 0, false
	}

	parsed, err := strconv.ParseUint(rawValue, 10, 64)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid member ID")
		return 0, false
	}
	return uint(parsed), true
}

func parseFailoverV2ExecutionID(c *gin.Context) (uint, bool) {
	rawValue := strings.TrimSpace(c.Param("execution_id"))
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

func parseFailoverV2PendingCleanupID(c *gin.Context) (uint, bool) {
	rawValue := strings.TrimSpace(c.Param("cleanup_id"))
	if rawValue == "" {
		api.RespondError(c, http.StatusBadRequest, "Pending cleanup ID is required")
		return 0, false
	}

	parsed, err := strconv.ParseUint(rawValue, 10, 64)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid pending cleanup ID")
		return 0, false
	}
	return uint(parsed), true
}

func parseFailoverV2EnabledRequest(c *gin.Context) (bool, bool) {
	var req failoverV2EnabledRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return false, false
	}
	if req.Enabled == nil {
		api.RespondError(c, http.StatusBadRequest, "enabled is required")
		return false, false
	}
	return *req.Enabled, true
}

func isFailoverV2ActiveExecutionError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "execution is active")
}

func buildFailoverV2ValidationCheck(key, label string, err error) failoverV2ValidationCheckView {
	check := failoverV2ValidationCheckView{
		Key:   strings.TrimSpace(key),
		Label: strings.TrimSpace(label),
	}
	if check.Label == "" {
		check.Label = check.Key
	}
	if err != nil {
		check.Status = "fail"
		check.Message = strings.TrimSpace(err.Error())
		return check
	}
	check.Status = "pass"
	return check
}

func buildFailoverV2ValidationWarning(key, label, message string) failoverV2ValidationCheckView {
	return failoverV2ValidationCheckView{
		Key:     strings.TrimSpace(key),
		Label:   strings.TrimSpace(label),
		Status:  "warn",
		Message: strings.TrimSpace(message),
	}
}

func buildFailoverV2ValidationView(checks []failoverV2ValidationCheckView) failoverV2ValidationView {
	view := failoverV2ValidationView{
		OK:     true,
		Checks: checks,
	}
	for _, check := range checks {
		if strings.TrimSpace(check.Status) == "fail" {
			view.OK = false
			break
		}
	}
	return view
}

func failoverV2ValidationHasWarnings(checks []failoverV2ValidationCheckView) bool {
	for _, check := range checks {
		if strings.TrimSpace(check.Status) == "warn" {
			return true
		}
	}
	return false
}

func validateFailoverV2ActiveExecutionCheck(userUUID string, serviceID uint) error {
	if serviceID == 0 {
		return nil
	}
	active, err := failoverv2db.HasActiveExecutionForService(userUUID, serviceID)
	if err != nil {
		return err
	}
	if active {
		return fmt.Errorf("another failover v2 execution is still active for this service")
	}
	return nil
}

func normalizeFailoverV2MemberModeRequest(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case models.FailoverV2MemberModeExistingClient:
		return models.FailoverV2MemberModeExistingClient
	case models.FailoverV2MemberModeProviderTemplate:
		return models.FailoverV2MemberModeProviderTemplate
	default:
		return models.FailoverV2MemberModeProviderTemplate
	}
}

func effectiveFailoverV2MemberLinesForView(member *models.FailoverV2Member) []models.FailoverV2MemberLine {
	if member == nil {
		return nil
	}
	if len(member.Lines) > 0 {
		return member.Lines
	}
	lineCode := strings.TrimSpace(member.DNSLine)
	if lineCode == "" {
		return nil
	}
	return []models.FailoverV2MemberLine{
		{
			ServiceID:     member.ServiceID,
			MemberID:      member.ID,
			LineCode:      lineCode,
			DNSRecordRefs: firstNonEmpty(strings.TrimSpace(member.DNSRecordRefs), "{}"),
		},
	}
}

func buildFailoverV2MemberLineViews(member *models.FailoverV2Member) []failoverV2MemberLineView {
	lines := effectiveFailoverV2MemberLinesForView(member)
	if len(lines) == 0 {
		return nil
	}

	views := make([]failoverV2MemberLineView, 0, len(lines))
	for _, line := range lines {
		views = append(views, failoverV2MemberLineView{
			LineCode:      strings.TrimSpace(line.LineCode),
			DNSRecordRefs: rawJSONOrNull(firstNonEmpty(strings.TrimSpace(line.DNSRecordRefs), "{}")),
		})
	}
	return views
}

func buildFailoverV2MemberDNSLines(member *models.FailoverV2Member) []string {
	lines := effectiveFailoverV2MemberLinesForView(member)
	if len(lines) == 0 {
		return nil
	}

	result := make([]string, 0, len(lines))
	for _, line := range lines {
		lineCode := strings.TrimSpace(line.LineCode)
		if lineCode != "" {
			result = append(result, lineCode)
		}
	}
	return result
}

func normalizeFailoverV2MemberDNSLines(req *failoverV2MemberRequest) []string {
	if req == nil {
		return nil
	}

	rawLines := req.DNSLines
	if len(rawLines) == 0 && strings.TrimSpace(req.DNSLine) != "" {
		rawLines = []string{req.DNSLine}
	}

	lines := make([]string, 0, len(rawLines))
	seen := make(map[string]struct{}, len(rawLines))
	for _, rawLine := range rawLines {
		lineCode := strings.TrimSpace(rawLine)
		if lineCode == "" {
			continue
		}
		key := strings.ToLower(lineCode)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		lines = append(lines, lineCode)
	}
	return lines
}

func buildFailoverV2MemberView(member models.FailoverV2Member) failoverV2MemberView {
	return failoverV2MemberView{
		ID:                  member.ID,
		ServiceID:           member.ServiceID,
		Name:                member.Name,
		Enabled:             member.Enabled,
		Priority:            member.Priority,
		Mode:                normalizeFailoverV2MemberModeRequest(member.Mode),
		WatchClientUUID:     member.WatchClientUUID,
		DNSLines:            buildFailoverV2MemberDNSLines(&member),
		Lines:               buildFailoverV2MemberLineViews(&member),
		DNSLine:             member.DNSLine,
		DNSRecordRefs:       rawJSONOrNull(member.DNSRecordRefs),
		CurrentAddress:      member.CurrentAddress,
		CurrentInstanceRef:  rawJSONOrNull(member.CurrentInstanceRef),
		Provider:            member.Provider,
		ProviderEntryID:     member.ProviderEntryID,
		ProviderEntryGroup:  member.ProviderEntryGroup,
		PlanPayload:         rawJSONOrNull(member.PlanPayload),
		FailureThreshold:    member.FailureThreshold,
		StaleAfterSeconds:   member.StaleAfterSeconds,
		CooldownSeconds:     member.CooldownSeconds,
		TriggerFailureCount: member.TriggerFailureCount,
		LastExecutionID:     member.LastExecutionID,
		LastStatus:          member.LastStatus,
		LastMessage:         member.LastMessage,
		LastTriggeredAt:     member.LastTriggeredAt,
		LastSucceededAt:     member.LastSucceededAt,
		LastFailedAt:        member.LastFailedAt,
		CreatedAt:           member.CreatedAt,
		UpdatedAt:           member.UpdatedAt,
	}
}

func buildFailoverV2ExecutionSummaryView(execution models.FailoverV2Execution) failoverV2ExecutionSummaryView {
	return failoverV2ExecutionSummaryView{
		ID:              execution.ID,
		ServiceID:       execution.ServiceID,
		MemberID:        execution.MemberID,
		Status:          execution.Status,
		TriggerReason:   execution.TriggerReason,
		TriggerSnapshot: rawJSONOrNull(execution.TriggerSnapshot),
		OldClientUUID:   execution.OldClientUUID,
		OldInstanceRef:  rawJSONOrNull(execution.OldInstanceRef),
		OldAddresses:    rawJSONOrNull(execution.OldAddresses),
		DetachDNSStatus: execution.DetachDNSStatus,
		DetachDNSResult: rawJSONOrNull(execution.DetachDNSResult),
		NewClientUUID:   execution.NewClientUUID,
		NewInstanceRef:  rawJSONOrNull(execution.NewInstanceRef),
		NewAddresses:    rawJSONOrNull(execution.NewAddresses),
		AttachDNSStatus: execution.AttachDNSStatus,
		AttachDNSResult: rawJSONOrNull(execution.AttachDNSResult),
		CleanupStatus:   execution.CleanupStatus,
		CleanupResult:   rawJSONOrNull(execution.CleanupResult),
		ErrorMessage:    execution.ErrorMessage,
		StartedAt:       execution.StartedAt,
		FinishedAt:      execution.FinishedAt,
	}
}

func buildFailoverV2ExecutionStepView(step models.FailoverV2ExecutionStep) failoverV2ExecutionStepView {
	return failoverV2ExecutionStepView{
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
	}
}

func buildFailoverV2ExecutionAvailableActionsView(service *models.FailoverV2Service, member *models.FailoverV2Member, execution *models.FailoverV2Execution) failoverV2ExecutionAvailableActionsView {
	availableActions := failoverv2svc.DescribeExecutionAvailableActions(service, member, execution)
	return failoverV2ExecutionAvailableActionsView{
		StopExecution: failoverV2ExecutionActionView{
			Available: availableActions.StopExecution.Available,
			Reason:    strings.TrimSpace(availableActions.StopExecution.Reason),
		},
		RetryAttachDNS: failoverV2ExecutionActionView{
			Available: availableActions.RetryAttachDNS.Available,
			Reason:    strings.TrimSpace(availableActions.RetryAttachDNS.Reason),
		},
		RetryCleanup: failoverV2ExecutionActionView{
			Available: availableActions.RetryCleanup.Available,
			Reason:    strings.TrimSpace(availableActions.RetryCleanup.Reason),
		},
	}
}

func buildFailoverV2ExecutionView(service *models.FailoverV2Service, member *models.FailoverV2Member, execution *models.FailoverV2Execution, includeSteps bool) failoverV2ExecutionView {
	view := failoverV2ExecutionView{
		ID:               execution.ID,
		ServiceID:        execution.ServiceID,
		MemberID:         execution.MemberID,
		Status:           execution.Status,
		TriggerReason:    execution.TriggerReason,
		TriggerSnapshot:  rawJSONOrNull(execution.TriggerSnapshot),
		OldClientUUID:    execution.OldClientUUID,
		OldInstanceRef:   rawJSONOrNull(execution.OldInstanceRef),
		OldAddresses:     rawJSONOrNull(execution.OldAddresses),
		DetachDNSStatus:  execution.DetachDNSStatus,
		DetachDNSResult:  rawJSONOrNull(execution.DetachDNSResult),
		NewClientUUID:    execution.NewClientUUID,
		NewInstanceRef:   rawJSONOrNull(execution.NewInstanceRef),
		NewAddresses:     rawJSONOrNull(execution.NewAddresses),
		AttachDNSStatus:  execution.AttachDNSStatus,
		AttachDNSResult:  rawJSONOrNull(execution.AttachDNSResult),
		CleanupStatus:    execution.CleanupStatus,
		CleanupResult:    rawJSONOrNull(execution.CleanupResult),
		ErrorMessage:     execution.ErrorMessage,
		AvailableActions: buildFailoverV2ExecutionAvailableActionsView(service, member, execution),
		StartedAt:        execution.StartedAt,
		FinishedAt:       execution.FinishedAt,
		CreatedAt:        execution.CreatedAt,
		UpdatedAt:        execution.UpdatedAt,
	}

	if includeSteps {
		view.Steps = make([]failoverV2ExecutionStepView, 0, len(execution.Steps))
		for _, step := range execution.Steps {
			view.Steps = append(view.Steps, buildFailoverV2ExecutionStepView(step))
		}
	}

	return view
}

func buildFailoverV2PendingCleanupAvailableActionsView(cleanup *models.FailoverV2PendingCleanup) failoverV2PendingCleanupAvailableActionsView {
	if cleanup == nil {
		return failoverV2PendingCleanupAvailableActionsView{
			Retry:            failoverV2PendingCleanupActionView{Reason: "pending cleanup is unavailable"},
			MarkResolved:     failoverV2PendingCleanupActionView{Reason: "pending cleanup is unavailable"},
			MarkManualReview: failoverV2PendingCleanupActionView{Reason: "pending cleanup is unavailable"},
		}
	}

	status := strings.TrimSpace(cleanup.Status)
	views := failoverV2PendingCleanupAvailableActionsView{
		Retry: failoverV2PendingCleanupActionView{
			Available: status != models.FailoverV2PendingCleanupStatusSucceeded && status != models.FailoverV2PendingCleanupStatusRunning,
		},
		MarkResolved: failoverV2PendingCleanupActionView{
			Available: status != models.FailoverV2PendingCleanupStatusSucceeded && status != models.FailoverV2PendingCleanupStatusRunning,
		},
		MarkManualReview: failoverV2PendingCleanupActionView{
			Available: status != models.FailoverV2PendingCleanupStatusSucceeded &&
				status != models.FailoverV2PendingCleanupStatusRunning &&
				status != models.FailoverV2PendingCleanupStatusManualReview,
		},
	}
	if !views.Retry.Available {
		if status == models.FailoverV2PendingCleanupStatusRunning {
			views.Retry.Reason = "pending cleanup retry is already running"
		} else {
			views.Retry.Reason = "pending cleanup is already resolved"
		}
	}
	if !views.MarkResolved.Available {
		if status == models.FailoverV2PendingCleanupStatusRunning {
			views.MarkResolved.Reason = "pending cleanup retry is already running"
		} else {
			views.MarkResolved.Reason = "pending cleanup is already resolved"
		}
	}
	if !views.MarkManualReview.Available {
		if status == models.FailoverV2PendingCleanupStatusManualReview {
			views.MarkManualReview.Reason = "pending cleanup is already marked for manual review"
		} else if status == models.FailoverV2PendingCleanupStatusRunning {
			views.MarkManualReview.Reason = "pending cleanup retry is already running"
		} else {
			views.MarkManualReview.Reason = "pending cleanup is already resolved"
		}
	}
	return views
}

func buildFailoverV2PendingCleanupView(cleanup models.FailoverV2PendingCleanup) failoverV2PendingCleanupView {
	return failoverV2PendingCleanupView{
		ID:               cleanup.ID,
		ServiceID:        cleanup.ServiceID,
		MemberID:         cleanup.MemberID,
		ExecutionID:      cleanup.ExecutionID,
		Provider:         cleanup.Provider,
		ProviderEntryID:  cleanup.ProviderEntryID,
		ResourceType:     cleanup.ResourceType,
		ResourceID:       cleanup.ResourceID,
		InstanceRef:      rawJSONOrNull(cleanup.InstanceRef),
		CleanupLabel:     cleanup.CleanupLabel,
		Status:           cleanup.Status,
		AttemptCount:     cleanup.AttemptCount,
		LastError:        cleanup.LastError,
		LastAttemptedAt:  cleanup.LastAttemptedAt,
		NextRetryAt:      cleanup.NextRetryAt,
		ResolvedAt:       cleanup.ResolvedAt,
		AvailableActions: buildFailoverV2PendingCleanupAvailableActionsView(&cleanup),
		CreatedAt:        cleanup.CreatedAt,
		UpdatedAt:        cleanup.UpdatedAt,
	}
}

func findFailoverV2MemberOnService(service *models.FailoverV2Service, memberID uint) *models.FailoverV2Member {
	if service == nil {
		return nil
	}
	for index := range service.Members {
		if service.Members[index].ID == memberID {
			return &service.Members[index]
		}
	}
	return nil
}

func loadFailoverV2ExecutionView(scope ownerScope, serviceID, executionID uint) (*failoverV2ExecutionView, error) {
	service, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID)
	if err != nil {
		return nil, err
	}

	execution, err := failoverv2db.GetExecutionByIDForUser(scope.UserUUID, serviceID, executionID)
	if err != nil {
		return nil, err
	}

	member := findFailoverV2MemberOnService(service, execution.MemberID)

	view := buildFailoverV2ExecutionView(service, member, execution, true)
	return &view, nil
}

func buildFailoverV2ServiceView(service *models.FailoverV2Service, recentExecutions []models.FailoverV2Execution) failoverV2ServiceView {
	members := make([]failoverV2MemberView, 0, len(service.Members))
	enabledMemberCount := 0
	for _, member := range service.Members {
		members = append(members, buildFailoverV2MemberView(member))
		if member.Enabled {
			enabledMemberCount++
		}
	}

	executions := make([]failoverV2ExecutionSummaryView, 0, len(recentExecutions))
	for _, execution := range recentExecutions {
		executions = append(executions, buildFailoverV2ExecutionSummaryView(execution))
	}

	return failoverV2ServiceView{
		ID:                  service.ID,
		Name:                service.Name,
		Enabled:             service.Enabled,
		DNSProvider:         service.DNSProvider,
		DNSEntryID:          service.DNSEntryID,
		DNSPayload:          rawJSONOrNull(service.DNSPayload),
		ScriptClipboardIDs:  rawJSONOrNull(service.ScriptClipboardIDs),
		ScriptTimeoutSec:    service.ScriptTimeoutSec,
		WaitAgentTimeoutSec: service.WaitAgentTimeoutSec,
		DeleteStrategy:      service.DeleteStrategy,
		DeleteDelaySeconds:  service.DeleteDelaySeconds,
		LastExecutionID:     service.LastExecutionID,
		LastStatus:          service.LastStatus,
		LastMessage:         service.LastMessage,
		MemberCount:         len(service.Members),
		EnabledMemberCount:  enabledMemberCount,
		Members:             members,
		RecentExecutions:    executions,
		CreatedAt:           service.CreatedAt,
		UpdatedAt:           service.UpdatedAt,
	}
}

func loadFailoverV2ServiceView(scope ownerScope, serviceID uint) (*failoverV2ServiceView, error) {
	service, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID)
	if err != nil {
		return nil, err
	}

	recentExecutions, err := failoverv2db.ListExecutionsByServiceForUser(scope.UserUUID, service.ID, 10)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	view := buildFailoverV2ServiceView(service, recentExecutions)
	return &view, nil
}

func normalizeFailoverV2ServiceScriptClipboardIDs(ids []int) ([]int, error) {
	normalized := models.NormalizeFailoverScriptClipboardIDs(nil, models.EncodeFailoverScriptClipboardIDs(ids))
	return normalized, nil
}

func rawFailoverV2JSONOrDefault(raw, fallback string) json.RawMessage {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return json.RawMessage(fallback)
	}
	return json.RawMessage(trimmed)
}

func buildFailoverV2ServiceRequestFromModel(service *models.FailoverV2Service) failoverV2ServiceRequest {
	enabled := false
	if service != nil {
		enabled = service.Enabled
	}
	if service == nil {
		return failoverV2ServiceRequest{Enabled: &enabled}
	}
	return failoverV2ServiceRequest{
		Name:                service.Name,
		Enabled:             &enabled,
		DNSProvider:         service.DNSProvider,
		DNSEntryID:          service.DNSEntryID,
		DNSPayload:          rawFailoverV2JSONOrDefault(service.DNSPayload, "{}"),
		ScriptClipboardIDs:  models.NormalizeFailoverScriptClipboardIDs(nil, service.ScriptClipboardIDs),
		ScriptTimeoutSec:    service.ScriptTimeoutSec,
		WaitAgentTimeoutSec: service.WaitAgentTimeoutSec,
		DeleteStrategy:      service.DeleteStrategy,
		DeleteDelaySeconds:  service.DeleteDelaySeconds,
	}
}

func buildFailoverV2MemberRequestFromModel(member *models.FailoverV2Member) failoverV2MemberRequest {
	enabled := false
	if member != nil {
		enabled = member.Enabled
	}
	if member == nil {
		return failoverV2MemberRequest{Enabled: &enabled}
	}
	return failoverV2MemberRequest{
		Name:               member.Name,
		Enabled:            &enabled,
		Priority:           member.Priority,
		Mode:               normalizeFailoverV2MemberModeRequest(member.Mode),
		WatchClientUUID:    member.WatchClientUUID,
		DNSLines:           buildFailoverV2MemberDNSLines(member),
		DNSLine:            member.DNSLine,
		DNSRecordRefs:      rawFailoverV2JSONOrDefault(member.DNSRecordRefs, "{}"),
		CurrentAddress:     member.CurrentAddress,
		CurrentInstanceRef: rawFailoverV2JSONOrDefault(member.CurrentInstanceRef, "null"),
		Provider:           member.Provider,
		ProviderEntryID:    member.ProviderEntryID,
		ProviderEntryGroup: member.ProviderEntryGroup,
		PlanPayload:        rawFailoverV2JSONOrDefault(member.PlanPayload, "{}"),
		FailureThreshold:   member.FailureThreshold,
		StaleAfterSeconds:  member.StaleAfterSeconds,
		CooldownSeconds:    member.CooldownSeconds,
	}
}

func validateFailoverV2ServiceRequest(scope ownerScope, req *failoverV2ServiceRequest) (*models.FailoverV2Service, error) {
	if req == nil {
		return nil, fmt.Errorf("request is required")
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	dnsProvider := strings.ToLower(strings.TrimSpace(req.DNSProvider))
	switch dnsProvider {
	case models.FailoverDNSProviderAliyun, models.FailoverDNSProviderCloudflare:
	default:
		return nil, fmt.Errorf("unsupported failover v2 dns provider: %s", req.DNSProvider)
	}

	dnsEntryID := strings.TrimSpace(req.DNSEntryID)
	if dnsEntryID == "" {
		return nil, fmt.Errorf("dns_entry_id is required")
	}
	if err := validateCloudProviderEntryForScope(scope, dnsProvider, dnsEntryID); err != nil {
		return nil, err
	}

	dnsPayload, err := normalizeJSONPayload(req.DNSPayload, "{}")
	if err != nil {
		return nil, fmt.Errorf("invalid dns_payload: %w", err)
	}
	if err := validateFailoverDNSPayload(scope, dnsProvider, dnsEntryID, dnsPayload); err != nil {
		return nil, err
	}

	scriptClipboardIDs, err := normalizeFailoverV2ServiceScriptClipboardIDs(req.ScriptClipboardIDs)
	if err != nil {
		return nil, err
	}
	for _, scriptClipboardID := range scriptClipboardIDs {
		if _, err := clipboarddb.GetClipboardByIDForUser(scriptClipboardID, scope.UserUUID); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, fmt.Errorf("script clipboard %d was not found", scriptClipboardID)
			}
			return nil, err
		}
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
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
		return nil, fmt.Errorf("unsupported delete strategy: %s", req.DeleteStrategy)
	}
	if req.DeleteDelaySeconds < 0 {
		return nil, fmt.Errorf("delete_delay_seconds must be greater than or equal to 0")
	}

	deleteDelaySeconds := req.DeleteDelaySeconds
	if deleteStrategy != models.FailoverDeleteStrategyDeleteAfterSuccessDelay {
		deleteDelaySeconds = 0
	}

	scriptTimeout := req.ScriptTimeoutSec
	if scriptTimeout <= 0 {
		scriptTimeout = 600
	}

	waitAgentTimeout := req.WaitAgentTimeoutSec
	if waitAgentTimeout <= 0 {
		waitAgentTimeout = 600
	}

	return &models.FailoverV2Service{
		Name:                name,
		Enabled:             enabled,
		DNSProvider:         dnsProvider,
		DNSEntryID:          dnsEntryID,
		DNSPayload:          dnsPayload,
		ScriptClipboardIDs:  models.EncodeFailoverScriptClipboardIDs(scriptClipboardIDs),
		ScriptTimeoutSec:    scriptTimeout,
		WaitAgentTimeoutSec: waitAgentTimeout,
		DeleteStrategy:      deleteStrategy,
		DeleteDelaySeconds:  deleteDelaySeconds,
	}, nil
}

func validateFailoverV2MemberRequest(scope ownerScope, service *models.FailoverV2Service, req *failoverV2MemberRequest) (*models.FailoverV2Member, error) {
	if req == nil {
		return nil, fmt.Errorf("request is required")
	}
	if service == nil {
		return nil, fmt.Errorf("service is required")
	}

	mode := normalizeFailoverV2MemberModeRequest(req.Mode)
	dnsLines := normalizeFailoverV2MemberDNSLines(req)
	if len(dnsLines) == 0 {
		return nil, fmt.Errorf("dns_lines is required")
	}

	watchClientUUID := strings.TrimSpace(req.WatchClientUUID)
	var client *models.Client
	if watchClientUUID != "" {
		loadedClient, err := clientdb.GetClientByUUIDForUser(watchClientUUID, scope.UserUUID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, fmt.Errorf("watch client not found")
			}
			return nil, err
		}
		client = &loadedClient
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = dnsLines[0]
	}

	dnsRecordRefs, err := normalizeJSONPayload(req.DNSRecordRefs, "{}")
	if err != nil {
		return nil, fmt.Errorf("invalid dns_record_refs: %w", err)
	}
	currentInstanceRef, err := normalizeJSONPayload(req.CurrentInstanceRef, "null")
	if err != nil {
		return nil, fmt.Errorf("invalid current_instance_ref: %w", err)
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	currentAddress := strings.TrimSpace(req.CurrentAddress)
	if currentAddress == "" && client != nil {
		currentAddress = strings.TrimSpace(firstNonEmpty(client.IPv4, client.IPv6))
	}

	if req.CooldownSeconds < 0 {
		return nil, fmt.Errorf("cooldown_seconds must be greater than or equal to 0")
	}

	provider := ""
	providerEntryID := ""
	providerEntryGroup := ""
	planPayload := "{}"

	switch mode {
	case models.FailoverV2MemberModeExistingClient:
		if watchClientUUID == "" {
			return nil, fmt.Errorf("watch_client_uuid is required for existing_client mode")
		}
	case models.FailoverV2MemberModeProviderTemplate:
		provider = strings.ToLower(strings.TrimSpace(req.Provider))
		switch provider {
		case digitalOceanProviderName, linodeProviderName, awsProviderName:
		default:
			return nil, fmt.Errorf("unsupported failover v2 member provider: %s", req.Provider)
		}

		providerEntryID = strings.TrimSpace(req.ProviderEntryID)
		if providerEntryID == "" {
			providerEntryID = "active"
		}
		providerEntryGroup = failoverv2svc.NormalizeProviderEntryGroup(req.ProviderEntryGroup)
		if err := validateFailoverProviderSelectionForScope(scope, provider, providerEntryID, providerEntryGroup); err != nil {
			return nil, err
		}

		planPayload, err = normalizeJSONPayload(req.PlanPayload, "{}")
		if err != nil {
			return nil, fmt.Errorf("invalid plan_payload: %w", err)
		}
		switch provider {
		case digitalOceanProviderName:
			if _, err := failoverv2svc.ParseDigitalOceanMemberPlanPayload(planPayload); err != nil {
				return nil, err
			}
		case linodeProviderName:
			if _, err := failoverv2svc.ParseLinodeMemberPlanPayload(planPayload); err != nil {
				return nil, err
			}
		case awsProviderName:
			if _, err := failoverv2svc.ParseAWSMemberPlanPayload(planPayload); err != nil {
				return nil, err
			}
		}
	}

	lines := make([]models.FailoverV2MemberLine, 0, len(dnsLines))
	for index, dnsLine := range dnsLines {
		lineRecordRefs := "{}"
		if index == 0 {
			lineRecordRefs = dnsRecordRefs
		}
		lines = append(lines, models.FailoverV2MemberLine{
			ServiceID:     service.ID,
			LineCode:      dnsLine,
			DNSRecordRefs: lineRecordRefs,
		})
	}

	return &models.FailoverV2Member{
		Name:               name,
		Enabled:            enabled,
		Priority:           req.Priority,
		Mode:               mode,
		WatchClientUUID:    watchClientUUID,
		DNSLine:            dnsLines[0],
		DNSRecordRefs:      dnsRecordRefs,
		CurrentAddress:     currentAddress,
		CurrentInstanceRef: currentInstanceRef,
		Provider:           provider,
		ProviderEntryID:    providerEntryID,
		ProviderEntryGroup: providerEntryGroup,
		PlanPayload:        planPayload,
		Lines:              lines,
		FailureThreshold:   req.FailureThreshold,
		StaleAfterSeconds:  req.StaleAfterSeconds,
		CooldownSeconds:    req.CooldownSeconds,
	}, nil
}

func ensureFailoverV2MemberUnique(service *models.FailoverV2Service, memberID uint, candidate *models.FailoverV2Member) error {
	if service == nil {
		return fmt.Errorf("service is required")
	}
	if candidate == nil {
		return fmt.Errorf("member is required")
	}

	watchClientUUID := strings.TrimSpace(candidate.WatchClientUUID)
	candidateLines := buildFailoverV2MemberDNSLines(candidate)
	candidateLineSet := make(map[string]struct{}, len(candidateLines))
	for _, line := range candidateLines {
		if lineCode := strings.ToLower(strings.TrimSpace(line)); lineCode != "" {
			candidateLineSet[lineCode] = struct{}{}
		}
	}

	for _, member := range service.Members {
		if member.ID == memberID {
			continue
		}
		if strings.TrimSpace(member.WatchClientUUID) != "" && strings.TrimSpace(member.WatchClientUUID) == strings.TrimSpace(watchClientUUID) {
			return fmt.Errorf("watch_client_uuid is already used by another member")
		}
		for _, existingLine := range buildFailoverV2MemberDNSLines(&member) {
			lineCode := strings.ToLower(strings.TrimSpace(existingLine))
			if lineCode == "" {
				continue
			}
			if _, exists := candidateLineSet[lineCode]; exists {
				return fmt.Errorf("dns line %q is already used by another member", existingLine)
			}
		}
	}
	return nil
}

func validateFailoverV2ExistingMembers(scope ownerScope, service *models.FailoverV2Service) error {
	if service == nil {
		return fmt.Errorf("service is required")
	}
	if len(service.Members) == 0 {
		return nil
	}
	for _, member := range service.Members {
		if !member.Enabled {
			continue
		}
		label := strings.TrimSpace(member.Name)
		if label == "" {
			label = firstNonEmpty(strings.TrimSpace(member.DNSLine), strings.Join(buildFailoverV2MemberDNSLines(&member), ","))
		}
		if label == "" {
			label = fmt.Sprintf("#%d", member.ID)
		}

		req := buildFailoverV2MemberRequestFromModel(&member)
		normalized, err := validateFailoverV2MemberRequest(scope, service, &req)
		if err != nil {
			return fmt.Errorf("member %s: %w", label, err)
		}
		if err := ensureFailoverV2MemberUnique(service, member.ID, normalized); err != nil {
			return fmt.Errorf("member %s: %w", label, err)
		}
		if err := failoverv2svc.EnsureMemberTargetAvailable(scope.UserUUID, normalized); err != nil {
			return fmt.Errorf("member %s: %w", label, err)
		}
	}
	return nil
}

func countEnabledFailoverV2Members(service *models.FailoverV2Service) int {
	if service == nil {
		return 0
	}
	count := 0
	for _, member := range service.Members {
		if member.Enabled {
			count++
		}
	}
	return count
}

func buildFailoverV2ExistingServiceValidation(scope ownerScope, service *models.FailoverV2Service) failoverV2ValidationView {
	checks := make([]failoverV2ValidationCheckView, 0, 5)
	if service == nil {
		checks = append(checks, buildFailoverV2ValidationCheck("service_exists", "Service exists", fmt.Errorf("service is required")))
		return buildFailoverV2ValidationView(checks)
	}

	req := buildFailoverV2ServiceRequestFromModel(service)
	normalized, err := validateFailoverV2ServiceRequest(scope, &req)
	checks = append(checks, buildFailoverV2ValidationCheck("service_config", "Service configuration", err))
	if err == nil {
		_, ownershipErr := failoverv2svc.EnsureServiceDNSOwnershipAvailable(scope.UserUUID, service.ID, normalized)
		checks = append(checks, buildFailoverV2ValidationCheck("dns_ownership", "DNS ownership and V1 conflict", ownershipErr))

		activeErr := validateFailoverV2ActiveExecutionCheck(scope.UserUUID, service.ID)
		checks = append(checks, buildFailoverV2ValidationCheck("active_execution", "No active execution", activeErr))
	}

	if countEnabledFailoverV2Members(service) == 0 {
		checks = append(checks, buildFailoverV2ValidationWarning(
			"members",
			"Existing members",
			"service has no enabled members",
		))
	} else {
		memberErr := validateFailoverV2ExistingMembers(scope, service)
		checks = append(checks, buildFailoverV2ValidationCheck("members", "Enabled member readiness", memberErr))
	}

	return buildFailoverV2ValidationView(checks)
}

func ValidateFailoverV2Service(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseOptionalFailoverV2ServiceID(c)
	if !ok {
		return
	}

	var req failoverV2ServiceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	checks := make([]failoverV2ValidationCheckView, 0, 5)
	var existing *models.FailoverV2Service
	if serviceID > 0 {
		loaded, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID)
		checks = append(checks, buildFailoverV2ValidationCheck("service_exists", "Service exists", err))
		if err == nil {
			existing = loaded
		}
	}

	service, err := validateFailoverV2ServiceRequest(scope, &req)
	checks = append(checks, buildFailoverV2ValidationCheck("service_config", "Service configuration", err))
	if err == nil {
		if serviceID == 0 || existing != nil {
			_, ownershipErr := failoverv2svc.EnsureServiceDNSOwnershipAvailable(scope.UserUUID, serviceID, service)
			checks = append(checks, buildFailoverV2ValidationCheck("dns_ownership", "DNS ownership and V1 conflict", ownershipErr))
		}
		if serviceID > 0 && existing != nil {
			activeErr := validateFailoverV2ActiveExecutionCheck(scope.UserUUID, serviceID)
			checks = append(checks, buildFailoverV2ValidationCheck("active_execution", "No active execution", activeErr))
		}
	}
	if existing != nil {
		if len(existing.Members) == 0 {
			checks = append(checks, buildFailoverV2ValidationWarning(
				"members",
				"Existing members",
				"service has no members yet",
			))
		} else {
			memberErr := validateFailoverV2ExistingMembers(scope, existing)
			checks = append(checks, buildFailoverV2ValidationCheck("members", "Existing member V1 conflicts", memberErr))
		}
	}

	api.RespondSuccess(c, buildFailoverV2ValidationView(checks))
}

func buildFailoverV2ServiceValidationView(service *models.FailoverV2Service, validation failoverV2ValidationView) failoverV2ServiceValidationView {
	view := failoverV2ServiceValidationView{
		OK:     validation.OK,
		Checks: validation.Checks,
	}
	if service != nil {
		view.ServiceID = service.ID
		view.ServiceName = strings.TrimSpace(service.Name)
		view.Enabled = service.Enabled
	}
	return view
}

func buildFailoverV2BulkValidationView(services []failoverV2ServiceValidationView) failoverV2BulkValidationView {
	view := failoverV2BulkValidationView{
		OK:       true,
		Checked:  len(services),
		Services: services,
	}
	if len(services) == 0 {
		view.Warnings = 1
	}
	for _, service := range services {
		if !service.OK {
			view.OK = false
			view.Failed++
		}
		if failoverV2ValidationHasWarnings(service.Checks) {
			view.Warnings++
		}
	}
	return view
}

func ValidateAllFailoverV2Services(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	services, err := failoverv2db.ListServicesByUser(scope.UserUUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list failover v2 services: "+err.Error())
		return
	}

	results := make([]failoverV2ServiceValidationView, 0, len(services))
	for index := range services {
		service := &services[index]
		if !service.Enabled {
			continue
		}
		validation := buildFailoverV2ExistingServiceValidation(scope, service)
		results = append(results, buildFailoverV2ServiceValidationView(service, validation))
	}

	api.RespondSuccess(c, buildFailoverV2BulkValidationView(results))
}

func ValidateFailoverV2Member(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	memberID, ok := parseOptionalFailoverV2MemberID(c)
	if !ok {
		return
	}

	var req failoverV2MemberRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	checks := make([]failoverV2ValidationCheckView, 0, 5)
	service, serviceErr := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID)
	checks = append(checks, buildFailoverV2ValidationCheck("service_exists", "Service exists", serviceErr))
	if serviceErr != nil {
		api.RespondSuccess(c, buildFailoverV2ValidationView(checks))
		return
	}
	if memberID > 0 && findFailoverV2MemberOnService(service, memberID) == nil {
		checks = append(checks, buildFailoverV2ValidationCheck(
			"member_exists",
			"Member exists",
			fmt.Errorf("failover v2 member not found"),
		))
	} else if memberID > 0 {
		checks = append(checks, buildFailoverV2ValidationCheck("member_exists", "Member exists", nil))
	}

	member, err := validateFailoverV2MemberRequest(scope, service, &req)
	checks = append(checks, buildFailoverV2ValidationCheck("member_config", "Member configuration", err))
	if err == nil {
		uniqueErr := ensureFailoverV2MemberUnique(service, memberID, member)
		checks = append(checks, buildFailoverV2ValidationCheck("member_unique", "Member uniqueness", uniqueErr))

		targetErr := failoverv2svc.EnsureMemberTargetAvailable(scope.UserUUID, member)
		checks = append(checks, buildFailoverV2ValidationCheck("v1_target", "V1 target conflict", targetErr))

		activeErr := validateFailoverV2ActiveExecutionCheck(scope.UserUUID, serviceID)
		checks = append(checks, buildFailoverV2ValidationCheck("active_execution", "No active execution", activeErr))
	}

	api.RespondSuccess(c, buildFailoverV2ValidationView(checks))
}

func GetFailoverV2Services(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	services, err := failoverv2db.ListServicesByUser(scope.UserUUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list failover v2 services: "+err.Error())
		return
	}

	response := make([]failoverV2ServiceView, 0, len(services))
	for i := range services {
		response = append(response, buildFailoverV2ServiceView(&services[i], nil))
	}
	api.RespondSuccess(c, response)
}

func CreateFailoverV2Service(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var req failoverV2ServiceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	service, err := validateFailoverV2ServiceRequest(scope, &req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := failoverv2svc.EnsureServiceDNSOwnershipAvailable(scope.UserUUID, 0, service); err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	created, err := failoverv2db.CreateServiceForUser(scope.UserUUID, service)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to create failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverV2ServiceView(created, nil))
}

func GetFailoverV2Service(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}

	view, err := loadFailoverV2ServiceView(scope, serviceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func GetFailoverV2Executions(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}

	if _, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 service: "+err.Error())
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

	executions, err := failoverv2db.ListExecutionsByServiceForUser(scope.UserUUID, serviceID, limit)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to list failover v2 executions: "+err.Error())
		return
	}

	response := make([]failoverV2ExecutionSummaryView, 0, len(executions))
	for _, execution := range executions {
		response = append(response, buildFailoverV2ExecutionSummaryView(execution))
	}
	api.RespondSuccess(c, response)
}

func GetFailoverV2PendingCleanups(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}

	if _, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 service: "+err.Error())
		return
	}

	limit := 50
	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			api.RespondError(c, http.StatusBadRequest, "Invalid limit")
			return
		}
		limit = parsed
	}

	var statuses []string
	if rawStatus := strings.TrimSpace(c.Query("status")); rawStatus != "" {
		for _, part := range strings.Split(rawStatus, ",") {
			if value := strings.TrimSpace(part); value != "" {
				statuses = append(statuses, value)
			}
		}
	}

	items, err := failoverv2db.ListPendingCleanupsByServiceForUser(scope.UserUUID, serviceID, limit, statuses)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to list failover v2 pending cleanups: "+err.Error())
		return
	}

	response := make([]failoverV2PendingCleanupView, 0, len(items))
	for _, item := range items {
		response = append(response, buildFailoverV2PendingCleanupView(item))
	}
	api.RespondSuccess(c, response)
}

func GetFailoverV2Execution(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	executionID, ok := parseFailoverV2ExecutionID(c)
	if !ok {
		return
	}

	view, err := loadFailoverV2ExecutionView(scope, serviceID, executionID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 execution not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 execution: "+err.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func RetryFailoverV2PendingCleanup(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	cleanupID, ok := parseFailoverV2PendingCleanupID(c)
	if !ok {
		return
	}

	cleanup, err := failoverv2svc.QueuePendingCleanupRetryForUser(scope.UserUUID, serviceID, cleanupID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 pending cleanup not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to retry failover v2 pending cleanup: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverV2PendingCleanupView(*cleanup))
}

func ResolveFailoverV2PendingCleanup(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	cleanupID, ok := parseFailoverV2PendingCleanupID(c)
	if !ok {
		return
	}

	cleanup, err := failoverv2svc.MarkPendingCleanupResolvedForUser(scope.UserUUID, serviceID, cleanupID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 pending cleanup not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to mark failover v2 pending cleanup resolved: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverV2PendingCleanupView(*cleanup))
}

func MarkFailoverV2PendingCleanupManualReview(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	cleanupID, ok := parseFailoverV2PendingCleanupID(c)
	if !ok {
		return
	}

	cleanup, err := failoverv2svc.MarkPendingCleanupManualReviewForUser(scope.UserUUID, serviceID, cleanupID, "")
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 pending cleanup not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to mark failover v2 pending cleanup for manual review: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverV2PendingCleanupView(*cleanup))
}

func RetryFailoverV2ExecutionAttachDNS(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	executionID, ok := parseFailoverV2ExecutionID(c)
	if !ok {
		return
	}

	execution, err := failoverv2svc.RetryAttachDNSForUser(scope.UserUUID, serviceID, executionID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 execution not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to retry failover v2 dns attach: "+err.Error())
		return
	}

	view, viewErr := loadFailoverV2ExecutionView(scope, serviceID, execution.ID)
	if viewErr != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 execution: "+viewErr.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func StopFailoverV2Execution(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	executionID, ok := parseFailoverV2ExecutionID(c)
	if !ok {
		return
	}

	execution, err := failoverv2svc.StopExecutionForUser(scope.UserUUID, serviceID, executionID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 execution not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to stop failover v2 execution: "+err.Error())
		return
	}

	view, viewErr := loadFailoverV2ExecutionView(scope, serviceID, execution.ID)
	if viewErr != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 execution: "+viewErr.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func RetryFailoverV2ExecutionCleanup(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	executionID, ok := parseFailoverV2ExecutionID(c)
	if !ok {
		return
	}

	execution, err := failoverv2svc.RetryCleanupForUser(scope.UserUUID, serviceID, executionID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 execution not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to retry failover v2 cleanup: "+err.Error())
		return
	}

	view, viewErr := loadFailoverV2ExecutionView(scope, serviceID, execution.ID)
	if viewErr != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 execution: "+viewErr.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func UpdateFailoverV2Service(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}

	var req failoverV2ServiceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	service, err := validateFailoverV2ServiceRequest(scope, &req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := failoverv2svc.EnsureServiceDNSOwnershipAvailable(scope.UserUUID, serviceID, service); err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if _, err := failoverv2db.UpdateServiceForUser(scope.UserUUID, serviceID, service); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		if isFailoverV2ActiveExecutionError(err) {
			api.RespondError(c, http.StatusBadRequest, "Failed to update failover v2 service: "+err.Error())
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to update failover v2 service: "+err.Error())
		return
	}

	view, err := loadFailoverV2ServiceView(scope, serviceID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func SetFailoverV2ServiceEnabled(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	enabled, ok := parseFailoverV2EnabledRequest(c)
	if !ok {
		return
	}

	if _, err := failoverv2db.SetServiceEnabledForUser(scope.UserUUID, serviceID, enabled); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to update failover v2 service state: "+err.Error())
		return
	}

	view, err := loadFailoverV2ServiceView(scope, serviceID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func DeleteFailoverV2Service(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}

	if err := failoverv2db.DeleteServiceForUser(scope.UserUUID, serviceID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		if isFailoverV2ActiveExecutionError(err) {
			api.RespondError(c, http.StatusBadRequest, "Failed to delete failover v2 service: "+err.Error())
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, nil)
}

func CreateFailoverV2Member(c *gin.Context) {
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
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 service: "+err.Error())
		return
	}

	var req failoverV2MemberRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	member, err := validateFailoverV2MemberRequest(scope, service, &req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := ensureFailoverV2MemberUnique(service, 0, member); err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if _, err := failoverv2db.CreateMemberForUser(scope.UserUUID, serviceID, member); err != nil {
		if isFailoverV2ActiveExecutionError(err) {
			api.RespondError(c, http.StatusBadRequest, "Failed to create failover v2 member: "+err.Error())
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to create failover v2 member: "+err.Error())
		return
	}

	view, err := loadFailoverV2ServiceView(scope, serviceID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func UpdateFailoverV2Member(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	memberID, ok := parseFailoverV2MemberID(c)
	if !ok {
		return
	}

	service, err := failoverv2db.GetServiceByIDForUser(scope.UserUUID, serviceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 service not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load failover v2 service: "+err.Error())
		return
	}

	var req failoverV2MemberRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	member, err := validateFailoverV2MemberRequest(scope, service, &req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := ensureFailoverV2MemberUnique(service, memberID, member); err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if _, err := failoverv2db.UpdateMemberForUser(scope.UserUUID, serviceID, memberID, member); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 member not found")
			return
		}
		if isFailoverV2ActiveExecutionError(err) {
			api.RespondError(c, http.StatusBadRequest, "Failed to update failover v2 member: "+err.Error())
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to update failover v2 member: "+err.Error())
		return
	}

	view, err := loadFailoverV2ServiceView(scope, serviceID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func SetFailoverV2MemberEnabled(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	memberID, ok := parseFailoverV2MemberID(c)
	if !ok {
		return
	}
	enabled, ok := parseFailoverV2EnabledRequest(c)
	if !ok {
		return
	}

	if _, err := failoverv2db.SetMemberEnabledForUser(scope.UserUUID, serviceID, memberID, enabled); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 member not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to update failover v2 member state: "+err.Error())
		return
	}

	view, err := loadFailoverV2ServiceView(scope, serviceID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func DeleteFailoverV2Member(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	memberID, ok := parseFailoverV2MemberID(c)
	if !ok {
		return
	}

	if err := failoverv2db.DeleteMemberForUser(scope.UserUUID, serviceID, memberID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 member not found")
			return
		}
		if isFailoverV2ActiveExecutionError(err) {
			api.RespondError(c, http.StatusBadRequest, "Failed to delete failover v2 member: "+err.Error())
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete failover v2 member: "+err.Error())
		return
	}

	view, err := loadFailoverV2ServiceView(scope, serviceID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to reload failover v2 service: "+err.Error())
		return
	}

	api.RespondSuccess(c, view)
}

func DetachFailoverV2MemberDNS(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	memberID, ok := parseFailoverV2MemberID(c)
	if !ok {
		return
	}

	execution, err := failoverv2svc.RunMemberDetachDNSNowForUser(scope.UserUUID, serviceID, memberID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 member not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to start failover v2 DNS detach: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverV2ExecutionSummaryView(*execution))
}

func RunFailoverV2Member(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	serviceID, ok := parseFailoverV2ServiceID(c)
	if !ok {
		return
	}
	memberID, ok := parseFailoverV2MemberID(c)
	if !ok {
		return
	}

	execution, err := failoverv2svc.RunMemberFailoverNowForUser(scope.UserUUID, serviceID, memberID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Failover v2 member not found")
			return
		}
		api.RespondError(c, http.StatusBadRequest, "Failed to start failover v2 execution: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildFailoverV2ExecutionSummaryView(*execution))
}
