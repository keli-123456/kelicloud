package models

import (
	"encoding/json"
	"strings"
)

const (
	FailoverTriggerSourceCNConnectivity = "cn_connectivity"
)

const (
	FailoverDNSProviderCloudflare = "cloudflare"
	FailoverDNSProviderAliyun     = "aliyun"
)

const (
	FailoverDeleteStrategyKeep                    = "keep"
	FailoverDeleteStrategyDeleteAfterSuccess      = "delete_after_success"
	FailoverDeleteStrategyDeleteAfterSuccessDelay = "delete_after_success_delay"
)

const (
	FailoverActionProvisionInstance = "provision_instance"
	FailoverActionRebindPublicIP    = "rebind_public_ip"
)

const (
	FailoverProvisionRetryLimitDefault           = 6
	FailoverProvisionFailureFallbackLimitDefault = 3
)

const (
	FailoverTaskStatusDisabled  = "disabled"
	FailoverTaskStatusHealthy   = "healthy"
	FailoverTaskStatusTriggered = "triggered"
	FailoverTaskStatusRunning   = "running"
	FailoverTaskStatusCooldown  = "cooldown"
	FailoverTaskStatusFailed    = "failed"
	FailoverTaskStatusUnknown   = "unknown"
)

const (
	FailoverExecutionStatusQueued        = "queued"
	FailoverExecutionStatusDetecting     = "detecting"
	FailoverExecutionStatusProvisioning  = "provisioning"
	FailoverExecutionStatusRebindingIP   = "rebinding_ip"
	FailoverExecutionStatusWaitingAgent  = "waiting_agent"
	FailoverExecutionStatusRunningScript = "running_script"
	FailoverExecutionStatusSwitchingDNS  = "switching_dns"
	FailoverExecutionStatusCleaningOld   = "cleaning_old"
	FailoverExecutionStatusSuccess       = "success"
	FailoverExecutionStatusFailed        = "failed"
)

const (
	FailoverStepStatusPending = "pending"
	FailoverStepStatusRunning = "running"
	FailoverStepStatusSuccess = "success"
	FailoverStepStatusFailed  = "failed"
	FailoverStepStatusSkipped = "skipped"
)

const (
	FailoverScriptStatusPending = "pending"
	FailoverScriptStatusRunning = "running"
	FailoverScriptStatusSuccess = "success"
	FailoverScriptStatusFailed  = "failed"
	FailoverScriptStatusTimeout = "timeout"
	FailoverScriptStatusSkipped = "skipped"
)

const (
	FailoverDNSStatusPending = "pending"
	FailoverDNSStatusSuccess = "success"
	FailoverDNSStatusFailed  = "failed"
	FailoverDNSStatusSkipped = "skipped"
)

const (
	FailoverCleanupStatusPending = "pending"
	FailoverCleanupStatusSuccess = "success"
	FailoverCleanupStatusFailed  = "failed"
	FailoverCleanupStatusSkipped = "skipped"
	FailoverCleanupStatusWarning = "warning"
)

const (
	FailoverPendingCleanupStatusPending      = "pending"
	FailoverPendingCleanupStatusSucceeded    = "succeeded"
	FailoverPendingCleanupStatusManualReview = "manual_review"
)

type FailoverTask struct {
	ID                            uint                `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID                        string              `json:"user_id,omitempty" gorm:"type:varchar(36);index"`
	Name                          string              `json:"name" gorm:"type:varchar(255);not null;index"`
	Enabled                       bool                `json:"enabled" gorm:"default:true;index:idx_failover_tasks_scheduler_enabled"`
	WatchClientUUID               string              `json:"watch_client_uuid" gorm:"type:varchar(36);not null;index"`
	CurrentAddress                string              `json:"current_address" gorm:"type:varchar(255)"`
	CurrentInstanceRef            string              `json:"current_instance_ref" gorm:"type:longtext"`
	TriggerFailureCount           int                 `json:"trigger_failure_count" gorm:"type:int;not null;default:0"`
	TriggerSource                 string              `json:"trigger_source" gorm:"type:varchar(64);not null;default:'cn_connectivity'"`
	FailureThreshold              int                 `json:"failure_threshold" gorm:"type:int;not null;default:2"`
	StaleAfterSeconds             int                 `json:"stale_after_seconds" gorm:"type:int;not null;default:300"`
	CooldownSeconds               int                 `json:"cooldown_seconds" gorm:"type:int;not null;default:0"`
	ProvisionRetryLimit           int                 `json:"provision_retry_limit" gorm:"type:int;not null;default:6"`
	ProvisionFailureFallbackLimit int                 `json:"provision_failure_fallback_limit" gorm:"type:int;not null;default:3"`
	DNSProvider                   string              `json:"dns_provider" gorm:"type:varchar(32);not null"`
	DNSEntryID                    string              `json:"dns_entry_id" gorm:"type:varchar(64);not null"`
	DNSPayload                    string              `json:"dns_payload" gorm:"type:longtext"`
	DeleteStrategy                string              `json:"delete_strategy" gorm:"type:varchar(64);not null;default:'keep'"`
	DeleteDelaySeconds            int                 `json:"delete_delay_seconds" gorm:"type:int;not null;default:0"`
	LastExecutionID               *uint               `json:"last_execution_id,omitempty" gorm:"index"`
	LastStatus                    string              `json:"last_status" gorm:"type:varchar(64);not null;default:'unknown'"`
	LastMessage                   string              `json:"last_message" gorm:"type:text"`
	LastTriggeredAt               *LocalTime          `json:"last_triggered_at" gorm:"type:timestamp"`
	LastSucceededAt               *LocalTime          `json:"last_succeeded_at" gorm:"type:timestamp"`
	LastFailedAt                  *LocalTime          `json:"last_failed_at" gorm:"type:timestamp"`
	Plans                         []FailoverPlan      `json:"plans,omitempty" gorm:"foreignKey:TaskID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	Executions                    []FailoverExecution `json:"executions,omitempty" gorm:"foreignKey:TaskID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	CreatedAt                     LocalTime           `json:"created_at"`
	UpdatedAt                     LocalTime           `json:"updated_at"`
}

type FailoverPlan struct {
	ID                  uint      `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	TaskID              uint      `json:"task_id" gorm:"not null;index"`
	Name                string    `json:"name" gorm:"type:varchar(255);not null"`
	Priority            int       `json:"priority" gorm:"type:int;not null;default:1"`
	Enabled             bool      `json:"enabled" gorm:"default:true"`
	Provider            string    `json:"provider" gorm:"type:varchar(32);not null;index"`
	ProviderEntryID     string    `json:"provider_entry_id" gorm:"type:varchar(64);not null"`
	ProviderEntryGroup  string    `json:"provider_entry_group" gorm:"type:varchar(100)"`
	ActionType          string    `json:"action_type" gorm:"type:varchar(64);not null"`
	Payload             string    `json:"payload" gorm:"type:longtext"`
	AutoConnectGroup    string    `json:"auto_connect_group" gorm:"type:varchar(100)"`
	ScriptClipboardID   *int      `json:"script_clipboard_id,omitempty" gorm:"index"`
	ScriptClipboardIDs  string    `json:"script_clipboard_ids,omitempty" gorm:"type:longtext"`
	ScriptTimeoutSec    int       `json:"script_timeout_sec" gorm:"type:int;not null;default:600"`
	WaitAgentTimeoutSec int       `json:"wait_agent_timeout_sec" gorm:"type:int;not null;default:600"`
	CreatedAt           LocalTime `json:"created_at"`
	UpdatedAt           LocalTime `json:"updated_at"`
}

type FailoverExecution struct {
	ID                    uint                    `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	TaskID                uint                    `json:"task_id" gorm:"not null;index"`
	Status                string                  `json:"status" gorm:"type:varchar(64);not null;default:'queued';index"`
	TriggerReason         string                  `json:"trigger_reason" gorm:"type:text"`
	WatchClientUUID       string                  `json:"watch_client_uuid" gorm:"type:varchar(36);not null;index"`
	TriggerSnapshot       string                  `json:"trigger_snapshot" gorm:"type:longtext"`
	SelectedPlanID        *uint                   `json:"selected_plan_id,omitempty" gorm:"index"`
	AttemptedPlans        string                  `json:"attempted_plans" gorm:"type:longtext"`
	OldClientUUID         string                  `json:"old_client_uuid" gorm:"type:varchar(36);index"`
	OldInstanceRef        string                  `json:"old_instance_ref" gorm:"type:longtext"`
	OldAddresses          string                  `json:"old_addresses" gorm:"type:longtext"`
	NewClientUUID         string                  `json:"new_client_uuid" gorm:"type:varchar(36);index"`
	NewInstanceRef        string                  `json:"new_instance_ref" gorm:"type:longtext"`
	NewAddresses          string                  `json:"new_addresses" gorm:"type:longtext"`
	ScriptClipboardID     *int                    `json:"script_clipboard_id,omitempty" gorm:"index"`
	ScriptClipboardIDs    string                  `json:"script_clipboard_ids,omitempty" gorm:"type:longtext"`
	ScriptNameSnapshot    string                  `json:"script_name_snapshot" gorm:"type:varchar(255)"`
	ScriptTaskID          string                  `json:"script_task_id" gorm:"type:varchar(64);index"`
	ScriptStatus          string                  `json:"script_status" gorm:"type:varchar(32);not null;default:'pending'"`
	ScriptExitCode        *int                    `json:"script_exit_code,omitempty" gorm:"type:int"`
	ScriptFinishedAt      *LocalTime              `json:"script_finished_at" gorm:"type:timestamp"`
	ScriptOutput          string                  `json:"script_output" gorm:"type:longtext"`
	ScriptOutputTruncated bool                    `json:"script_output_truncated" gorm:"default:false"`
	DNSProvider           string                  `json:"dns_provider" gorm:"type:varchar(32)"`
	DNSStatus             string                  `json:"dns_status" gorm:"type:varchar(32);not null;default:'pending'"`
	DNSResult             string                  `json:"dns_result" gorm:"type:longtext"`
	CleanupStatus         string                  `json:"cleanup_status" gorm:"type:varchar(32);not null;default:'pending'"`
	CleanupResult         string                  `json:"cleanup_result" gorm:"type:longtext"`
	ErrorMessage          string                  `json:"error_message" gorm:"type:longtext"`
	StartedAt             LocalTime               `json:"started_at" gorm:"type:timestamp;index"`
	FinishedAt            *LocalTime              `json:"finished_at" gorm:"type:timestamp"`
	Steps                 []FailoverExecutionStep `json:"steps,omitempty" gorm:"foreignKey:ExecutionID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	CreatedAt             LocalTime               `json:"created_at"`
	UpdatedAt             LocalTime               `json:"updated_at"`
}

type FailoverPendingCleanup struct {
	ID              uint       `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID          string     `json:"user_id" gorm:"type:varchar(36);index;uniqueIndex:idx_failover_pending_cleanup_resource"`
	TaskID          uint       `json:"task_id,omitempty" gorm:"index"`
	ExecutionID     uint       `json:"execution_id,omitempty" gorm:"index"`
	Provider        string     `json:"provider" gorm:"type:varchar(32);not null;index;uniqueIndex:idx_failover_pending_cleanup_resource"`
	ProviderEntryID string     `json:"provider_entry_id" gorm:"type:varchar(64);index"`
	ResourceType    string     `json:"resource_type" gorm:"type:varchar(32);not null;uniqueIndex:idx_failover_pending_cleanup_resource"`
	ResourceID      string     `json:"resource_id" gorm:"type:varchar(128);not null;index;uniqueIndex:idx_failover_pending_cleanup_resource"`
	InstanceRef     string     `json:"instance_ref" gorm:"type:longtext"`
	CleanupLabel    string     `json:"cleanup_label" gorm:"type:varchar(255)"`
	Status          string     `json:"status" gorm:"type:varchar(32);not null;default:'pending';index"`
	AttemptCount    int        `json:"attempt_count" gorm:"type:int;not null;default:0"`
	LastError       string     `json:"last_error" gorm:"type:longtext"`
	LastAttemptedAt *LocalTime `json:"last_attempted_at" gorm:"type:timestamp"`
	NextRetryAt     *LocalTime `json:"next_retry_at" gorm:"type:timestamp;index"`
	ResolvedAt      *LocalTime `json:"resolved_at" gorm:"type:timestamp"`
	CreatedAt       LocalTime  `json:"created_at"`
	UpdatedAt       LocalTime  `json:"updated_at"`
}

func NormalizeFailoverScriptClipboardIDs(primary *int, raw string) []int {
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" {
		var parsed []int
		if err := json.Unmarshal([]byte(trimmed), &parsed); err == nil {
			return normalizeFailoverScriptClipboardIDList(parsed)
		}
	}

	if primary == nil || *primary <= 0 {
		return nil
	}
	return []int{*primary}
}

func EncodeFailoverScriptClipboardIDs(ids []int) string {
	normalized := normalizeFailoverScriptClipboardIDList(ids)
	if len(normalized) == 0 {
		return ""
	}

	payload, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(payload)
}

func FirstFailoverScriptClipboardID(ids []int) *int {
	normalized := normalizeFailoverScriptClipboardIDList(ids)
	if len(normalized) == 0 {
		return nil
	}

	id := normalized[0]
	return &id
}

func (p FailoverPlan) EffectiveScriptClipboardIDs() []int {
	return NormalizeFailoverScriptClipboardIDs(p.ScriptClipboardID, p.ScriptClipboardIDs)
}

func (e FailoverExecution) EffectiveScriptClipboardIDs() []int {
	return NormalizeFailoverScriptClipboardIDs(e.ScriptClipboardID, e.ScriptClipboardIDs)
}

func normalizeFailoverScriptClipboardIDList(ids []int) []int {
	if len(ids) == 0 {
		return nil
	}

	normalized := make([]int, 0, len(ids))
	seen := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
	}

	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

type FailoverExecutionStep struct {
	ID          uint       `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	ExecutionID uint       `json:"execution_id" gorm:"not null;index"`
	Sort        int        `json:"sort" gorm:"type:int;not null;default:0"`
	StepKey     string     `json:"step_key" gorm:"type:varchar(64);not null;index"`
	StepLabel   string     `json:"step_label" gorm:"type:varchar(255);not null"`
	Status      string     `json:"status" gorm:"type:varchar(32);not null;default:'pending'"`
	Message     string     `json:"message" gorm:"type:text"`
	Detail      string     `json:"detail" gorm:"type:longtext"`
	StartedAt   *LocalTime `json:"started_at" gorm:"type:timestamp"`
	FinishedAt  *LocalTime `json:"finished_at" gorm:"type:timestamp"`
	CreatedAt   LocalTime  `json:"created_at"`
	UpdatedAt   LocalTime  `json:"updated_at"`
}
