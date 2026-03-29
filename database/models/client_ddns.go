package models

const (
	ClientDDNSAddressModeIPv4 = "ipv4"
	ClientDDNSAddressModeIPv6 = "ipv6"
	ClientDDNSAddressModeDual = "dual"
)

type ClientDDNSBinding struct {
	ID           uint       `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	UserID       string     `json:"user_id,omitempty" gorm:"type:varchar(36);index"`
	ClientUUID   string     `json:"client_uuid" gorm:"type:varchar(36);not null;uniqueIndex"`
	Enabled      bool       `json:"enabled" gorm:"default:true"`
	Provider     string     `json:"provider" gorm:"type:varchar(32);not null;index"`
	EntryID      string     `json:"entry_id" gorm:"type:varchar(64);not null"`
	AddressMode  string     `json:"address_mode" gorm:"type:varchar(16);not null;default:'ipv4'"`
	Payload      string     `json:"payload" gorm:"type:longtext"`
	RecordKey    string     `json:"record_key" gorm:"type:varchar(512);index"`
	LastIPv4     string     `json:"last_ipv4" gorm:"type:varchar(100)"`
	LastIPv6     string     `json:"last_ipv6" gorm:"type:varchar(100)"`
	LastSyncedAt *LocalTime `json:"last_synced_at" gorm:"type:timestamp"`
	LastError    string     `json:"last_error" gorm:"type:text"`
	LastResult   string     `json:"last_result" gorm:"type:longtext"`
	CreatedAt    LocalTime  `json:"created_at"`
	UpdatedAt    LocalTime  `json:"updated_at"`
}
