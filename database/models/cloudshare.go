package models

import "time"

type CloudInstanceShare struct {
	ID                 uint       `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID             string     `json:"user_id,omitempty" gorm:"type:varchar(36);uniqueIndex:idx_cloud_instance_shares_scope_resource"`
	ShareToken         string     `json:"share_token" gorm:"type:varchar(64);uniqueIndex;not null"`
	Provider           string     `json:"provider" gorm:"type:varchar(32);uniqueIndex:idx_cloud_instance_shares_scope_resource;not null"`
	ResourceType       string     `json:"resource_type" gorm:"type:varchar(32);uniqueIndex:idx_cloud_instance_shares_scope_resource;not null"`
	ResourceID         string     `json:"resource_id" gorm:"type:varchar(191);uniqueIndex:idx_cloud_instance_shares_scope_resource;not null"`
	ResourceName       string     `json:"resource_name" gorm:"type:varchar(255)"`
	CredentialID       string     `json:"credential_id" gorm:"type:varchar(191);index"`
	Region             string     `json:"region" gorm:"type:varchar(64)"`
	Title              string     `json:"title" gorm:"type:varchar(255)"`
	Note               string     `json:"note" gorm:"type:longtext"`
	AccessPolicy       string     `json:"access_policy" gorm:"type:varchar(32);not null;default:'public'"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty" gorm:"index"`
	LastAccessedAt     *time.Time `json:"last_accessed_at,omitempty"`
	ConsumedAt         *time.Time `json:"consumed_at,omitempty" gorm:"index"`
	AccessCount        int        `json:"access_count" gorm:"not null;default:0"`
	SharePassword      bool       `json:"share_password" gorm:"default:false"`
	ShareManagedSSHKey bool       `json:"share_managed_ssh_key" gorm:"default:false"`
	CreatedAt          time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt          time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
}
