package models

const (
	FailoverV2ServiceStatusUnknown = "unknown"
	FailoverV2ServiceStatusHealthy = "healthy"
	FailoverV2ServiceStatusRunning = "running"
	FailoverV2ServiceStatusFailed  = "failed"
)

const (
	FailoverV2DefaultCheckIntervalSeconds = 60
	FailoverV2MinCheckIntervalSeconds     = 60
)

const (
	FailoverV2ExecutionStatusQueued             = "queued"
	FailoverV2ExecutionStatusDetachingDNS       = "detaching_dns"
	FailoverV2ExecutionStatusVerifyingDetachDNS = "verifying_detach_dns"
	FailoverV2ExecutionStatusProvisioning       = "provisioning"
	FailoverV2ExecutionStatusWaitingAgent       = "waiting_agent"
	FailoverV2ExecutionStatusValidatingOutlet   = "validating_outlet"
	FailoverV2ExecutionStatusRunningScripts     = "running_scripts"
	FailoverV2ExecutionStatusAttachingDNS       = "attaching_dns"
	FailoverV2ExecutionStatusVerifyingAttachDNS = "verifying_attach_dns"
	FailoverV2ExecutionStatusCleaningOld        = "cleaning_old"
	FailoverV2ExecutionStatusSuccess            = "success"
	FailoverV2ExecutionStatusFailed             = "failed"
)

const (
	FailoverV2MemberStatusDisabled  = "disabled"
	FailoverV2MemberStatusHealthy   = "healthy"
	FailoverV2MemberStatusTriggered = "triggered"
	FailoverV2MemberStatusRunning   = "running"
	FailoverV2MemberStatusCooldown  = "cooldown"
	FailoverV2MemberStatusFailed    = "failed"
	FailoverV2MemberStatusUnknown   = "unknown"
)

const (
	FailoverV2MemberModeExistingClient   = "existing_client"
	FailoverV2MemberModeProviderTemplate = "provider_template"
)

const (
	FailoverV2PendingCleanupStatusPending      = "pending"
	FailoverV2PendingCleanupStatusRunning      = "running"
	FailoverV2PendingCleanupStatusSucceeded    = "succeeded"
	FailoverV2PendingCleanupStatusManualReview = "manual_review"
)

type FailoverV2Service struct {
	ID                   uint                  `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID               string                `json:"user_id,omitempty" gorm:"type:varchar(36);index"`
	Name                 string                `json:"name" gorm:"type:varchar(255);not null;index"`
	Enabled              bool                  `json:"enabled" gorm:"default:true;index:idx_failover_v2_scheduler_due,priority:1"`
	DNSProvider          string                `json:"dns_provider" gorm:"type:varchar(32);not null"`
	DNSEntryID           string                `json:"dns_entry_id" gorm:"type:varchar(64);not null"`
	DNSPayload           string                `json:"dns_payload" gorm:"type:longtext"`
	ScriptClipboardIDs   string                `json:"script_clipboard_ids,omitempty" gorm:"type:longtext"`
	ScriptTimeoutSec     int                   `json:"script_timeout_sec" gorm:"type:int;not null;default:600"`
	WaitAgentTimeoutSec  int                   `json:"wait_agent_timeout_sec" gorm:"type:int;not null;default:600"`
	CheckIntervalSeconds int                   `json:"check_interval_seconds" gorm:"type:int;not null;default:60"`
	DeleteStrategy       string                `json:"delete_strategy" gorm:"type:varchar(64);not null;default:'keep'"`
	DeleteDelaySeconds   int                   `json:"delete_delay_seconds" gorm:"type:int;not null;default:0"`
	LastExecutionID      *uint                 `json:"last_execution_id,omitempty" gorm:"index"`
	LastCheckedAt        *LocalTime            `json:"last_checked_at" gorm:"type:timestamp;index:idx_failover_v2_scheduler_due,priority:2"`
	LastStatus           string                `json:"last_status" gorm:"type:varchar(64);not null;default:'unknown'"`
	LastMessage          string                `json:"last_message" gorm:"type:text"`
	Members              []FailoverV2Member    `json:"members,omitempty" gorm:"foreignKey:ServiceID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	Executions           []FailoverV2Execution `json:"executions,omitempty" gorm:"foreignKey:ServiceID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	CreatedAt            LocalTime             `json:"created_at"`
	UpdatedAt            LocalTime             `json:"updated_at"`
}

type FailoverV2Member struct {
	ID                  uint                   `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	ServiceID           uint                   `json:"service_id" gorm:"not null;index;index:idx_failover_v2_service_line,priority:1;index:idx_failover_v2_service_client;index:idx_failover_v2_member_cooldown,priority:3"`
	Name                string                 `json:"name" gorm:"type:varchar(255);not null"`
	Enabled             bool                   `json:"enabled" gorm:"default:true;index:idx_failover_v2_member_cooldown,priority:1"`
	Priority            int                    `json:"priority" gorm:"type:int;not null;default:1"`
	Mode                string                 `json:"mode" gorm:"type:varchar(32);not null;default:'provider_template'"`
	WatchClientUUID     string                 `json:"watch_client_uuid" gorm:"type:varchar(36);index;index:idx_failover_v2_service_client"`
	DNSLine             string                 `json:"dns_line" gorm:"type:varchar(64);not null;index:idx_failover_v2_service_line,priority:2"`
	DNSRecordRefs       string                 `json:"dns_record_refs" gorm:"type:longtext"`
	CurrentAddress      string                 `json:"current_address" gorm:"type:varchar(255)"`
	CurrentInstanceRef  string                 `json:"current_instance_ref" gorm:"type:longtext"`
	Provider            string                 `json:"provider" gorm:"type:varchar(32);not null;index"`
	ProviderEntryID     string                 `json:"provider_entry_id" gorm:"type:varchar(64);not null"`
	ProviderEntryGroup  string                 `json:"provider_entry_group" gorm:"type:varchar(100)"`
	PlanPayload         string                 `json:"plan_payload" gorm:"type:longtext"`
	FailureThreshold    int                    `json:"failure_threshold" gorm:"type:int;not null;default:2"`
	StaleAfterSeconds   int                    `json:"stale_after_seconds" gorm:"type:int;not null;default:300"`
	CooldownSeconds     int                    `json:"cooldown_seconds" gorm:"type:int;not null;default:0"`
	TriggerFailureCount int                    `json:"trigger_failure_count" gorm:"type:int;not null;default:0"`
	LastExecutionID     *uint                  `json:"last_execution_id,omitempty" gorm:"index"`
	LastStatus          string                 `json:"last_status" gorm:"type:varchar(64);not null;default:'unknown';index:idx_failover_v2_member_cooldown,priority:2"`
	LastMessage         string                 `json:"last_message" gorm:"type:text"`
	LastTriggeredAt     *LocalTime             `json:"last_triggered_at" gorm:"type:timestamp;index:idx_failover_v2_member_cooldown,priority:4"`
	LastSucceededAt     *LocalTime             `json:"last_succeeded_at" gorm:"type:timestamp"`
	LastFailedAt        *LocalTime             `json:"last_failed_at" gorm:"type:timestamp"`
	Lines               []FailoverV2MemberLine `json:"lines,omitempty" gorm:"foreignKey:MemberID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	CreatedAt           LocalTime              `json:"created_at"`
	UpdatedAt           LocalTime              `json:"updated_at"`
}

type FailoverV2MemberLine struct {
	ID            uint      `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	ServiceID     uint      `json:"service_id" gorm:"not null;index;index:idx_failover_v2_service_line_code,priority:1"`
	MemberID      uint      `json:"member_id" gorm:"not null;index;uniqueIndex:idx_failover_v2_member_line_code,priority:1"`
	LineCode      string    `json:"line_code" gorm:"type:varchar(64);not null;index:idx_failover_v2_service_line_code,priority:2;uniqueIndex:idx_failover_v2_member_line_code,priority:2"`
	DNSRecordRefs string    `json:"dns_record_refs" gorm:"type:longtext"`
	CreatedAt     LocalTime `json:"created_at"`
	UpdatedAt     LocalTime `json:"updated_at"`
}

type FailoverV2Execution struct {
	ID              uint                      `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	ServiceID       uint                      `json:"service_id" gorm:"not null;index"`
	MemberID        uint                      `json:"member_id" gorm:"not null;index"`
	Status          string                    `json:"status" gorm:"type:varchar(64);not null;default:'queued';index"`
	TriggerReason   string                    `json:"trigger_reason" gorm:"type:text"`
	TriggerSnapshot string                    `json:"trigger_snapshot" gorm:"type:longtext"`
	OldClientUUID   string                    `json:"old_client_uuid" gorm:"type:varchar(36);index"`
	OldInstanceRef  string                    `json:"old_instance_ref" gorm:"type:longtext"`
	OldAddresses    string                    `json:"old_addresses" gorm:"type:longtext"`
	DetachDNSStatus string                    `json:"detach_dns_status" gorm:"type:varchar(32);not null;default:'pending'"`
	DetachDNSResult string                    `json:"detach_dns_result" gorm:"type:longtext"`
	NewClientUUID   string                    `json:"new_client_uuid" gorm:"type:varchar(36);index"`
	NewInstanceRef  string                    `json:"new_instance_ref" gorm:"type:longtext"`
	NewAddresses    string                    `json:"new_addresses" gorm:"type:longtext"`
	AttachDNSStatus string                    `json:"attach_dns_status" gorm:"type:varchar(32);not null;default:'pending'"`
	AttachDNSResult string                    `json:"attach_dns_result" gorm:"type:longtext"`
	CleanupStatus   string                    `json:"cleanup_status" gorm:"type:varchar(32);not null;default:'pending'"`
	CleanupResult   string                    `json:"cleanup_result" gorm:"type:longtext"`
	ErrorMessage    string                    `json:"error_message" gorm:"type:longtext"`
	StartedAt       LocalTime                 `json:"started_at" gorm:"type:timestamp;index"`
	FinishedAt      *LocalTime                `json:"finished_at" gorm:"type:timestamp"`
	Steps           []FailoverV2ExecutionStep `json:"steps,omitempty" gorm:"foreignKey:ExecutionID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	CreatedAt       LocalTime                 `json:"created_at"`
	UpdatedAt       LocalTime                 `json:"updated_at"`
}

type FailoverV2ExecutionStep struct {
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

type FailoverV2PendingCleanup struct {
	ID              uint       `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID          string     `json:"user_id" gorm:"type:varchar(36);index;uniqueIndex:idx_failover_v2_pending_cleanup_resource"`
	ServiceID       uint       `json:"service_id,omitempty" gorm:"index"`
	MemberID        uint       `json:"member_id,omitempty" gorm:"index"`
	ExecutionID     uint       `json:"execution_id,omitempty" gorm:"index"`
	Provider        string     `json:"provider" gorm:"type:varchar(32);not null;index;uniqueIndex:idx_failover_v2_pending_cleanup_resource"`
	ProviderEntryID string     `json:"provider_entry_id" gorm:"type:varchar(64);index"`
	ResourceType    string     `json:"resource_type" gorm:"type:varchar(32);not null;uniqueIndex:idx_failover_v2_pending_cleanup_resource"`
	ResourceID      string     `json:"resource_id" gorm:"type:varchar(128);not null;index;uniqueIndex:idx_failover_v2_pending_cleanup_resource"`
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

type FailoverV2RunLock struct {
	ID        uint      `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	LockKey   string    `json:"lock_key" gorm:"type:varchar(255);not null;uniqueIndex"`
	Owner     string    `json:"owner" gorm:"type:varchar(128);not null;index"`
	ExpiresAt LocalTime `json:"expires_at" gorm:"type:timestamp;index"`
	CreatedAt LocalTime `json:"created_at"`
	UpdatedAt LocalTime `json:"updated_at"`
}
