package models

const (
	AWSFollowUpTaskTypeEC2AssignIPv6          = "ec2_assign_ipv6"
	AWSFollowUpTaskTypeEC2AllowAllTraffic     = "ec2_allow_all_traffic"
	AWSFollowUpTaskTypeLightsailAllowAllPorts = "lightsail_allow_all_ports"
)

const (
	AWSFollowUpTaskStatusPending   = "pending"
	AWSFollowUpTaskStatusSuccess   = "success"
	AWSFollowUpTaskStatusFailed    = "failed"
	AWSFollowUpTaskStatusCancelled = "cancelled"
	AWSFollowUpTaskStatusSkipped   = "skipped"
)

const AWSFollowUpTaskMaxAttemptsDefault = 60

type AWSFollowUpTask struct {
	ID            uint       `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID        string     `json:"user_id,omitempty" gorm:"type:varchar(36);not null;index"`
	CredentialID  string     `json:"credential_id" gorm:"type:varchar(64);not null;index"`
	Region        string     `json:"region" gorm:"type:varchar(64);not null;index"`
	TaskType      string     `json:"task_type" gorm:"type:varchar(64);not null;index"`
	ResourceID    string     `json:"resource_id" gorm:"type:varchar(255);not null;index"`
	Status        string     `json:"status" gorm:"type:varchar(32);not null;default:'pending';index"`
	Attempts      int        `json:"attempts" gorm:"type:int;not null;default:0"`
	MaxAttempts   int        `json:"max_attempts" gorm:"type:int;not null;default:60"`
	LastError     string     `json:"last_error" gorm:"type:text"`
	LastAttemptAt *LocalTime `json:"last_attempt_at" gorm:"type:timestamp"`
	LeaseUntil    *LocalTime `json:"lease_until" gorm:"type:timestamp;index"`
	NextRunAt     LocalTime  `json:"next_run_at" gorm:"type:timestamp;not null;index"`
	CompletedAt   *LocalTime `json:"completed_at" gorm:"type:timestamp"`
	CreatedAt     LocalTime  `json:"created_at"`
	UpdatedAt     LocalTime  `json:"updated_at"`
}
