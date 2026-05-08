package models

import "time"

type FailoverShare struct {
	ID             uint       `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID         string     `json:"user_id,omitempty" gorm:"type:varchar(36);uniqueIndex:idx_failover_shares_scope_task"`
	ShareToken     string     `json:"share_token" gorm:"type:varchar(64);uniqueIndex;not null"`
	TaskID         uint       `json:"task_id" gorm:"not null;index;uniqueIndex:idx_failover_shares_scope_task"`
	TaskName       string     `json:"task_name" gorm:"type:varchar(255)"`
	Title          string     `json:"title" gorm:"type:varchar(255)"`
	Note           string     `json:"note" gorm:"type:longtext"`
	AccessPolicy   string     `json:"access_policy" gorm:"type:varchar(32);not null;default:'public'"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty" gorm:"index"`
	LastAccessedAt *time.Time `json:"last_accessed_at,omitempty"`
	ConsumedAt     *time.Time `json:"consumed_at,omitempty" gorm:"index"`
	AccessCount    int        `json:"access_count" gorm:"not null;default:0"`
	CreatedAt      time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt      time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
}
