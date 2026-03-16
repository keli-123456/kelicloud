package models

type TenantInvite struct {
	ID          string     `json:"id" gorm:"type:varchar(36);primaryKey"`
	TenantID    string     `json:"tenant_id" gorm:"type:varchar(36);index;not null"`
	Token       string     `json:"token" gorm:"type:varchar(64);uniqueIndex;not null"`
	InviterUUID string     `json:"inviter_uuid" gorm:"type:varchar(36);index"`
	Role        string     `json:"role" gorm:"type:varchar(20);not null;default:'viewer'"`
	AcceptedBy  string     `json:"accepted_by" gorm:"type:varchar(36);index"`
	ExpiresAt   *LocalTime `json:"expires_at" gorm:"type:timestamp"`
	AcceptedAt  *LocalTime `json:"accepted_at" gorm:"type:timestamp"`
	RevokedAt   *LocalTime `json:"revoked_at" gorm:"type:timestamp"`
	CreatedAt   LocalTime  `json:"created_at"`
	UpdatedAt   LocalTime  `json:"updated_at"`
}
