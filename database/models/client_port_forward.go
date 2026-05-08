package models

const (
	ClientPortForwardProtocolTCP = "tcp"
	ClientPortForwardProtocolUDP = "udp"
)

type ClientPortForwardRule struct {
	ID            uint       `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID        string     `json:"user_id,omitempty" gorm:"type:varchar(36);index"`
	ClientUUID    string     `json:"client_uuid" gorm:"type:varchar(36);not null;index"`
	Name          string     `json:"name" gorm:"type:varchar(100)"`
	Enabled       bool       `json:"enabled" gorm:"default:true"`
	Protocol      string     `json:"protocol" gorm:"type:varchar(8);not null;default:'tcp';index"`
	ListenPort    int        `json:"listen_port" gorm:"not null;index"`
	TargetHost    string     `json:"target_host" gorm:"type:varchar(255);not null"`
	TargetPort    int        `json:"target_port" gorm:"not null"`
	LastTaskID    string     `json:"last_task_id" gorm:"type:varchar(36)"`
	LastAppliedAt *LocalTime `json:"last_applied_at" gorm:"type:timestamp"`
	LastError     string     `json:"last_error" gorm:"type:text"`
	CreatedAt     LocalTime  `json:"created_at"`
	UpdatedAt     LocalTime  `json:"updated_at"`
}
