package models

type CloudProvider struct {
	TenantID string `json:"tenant_id" gorm:"type:varchar(36);primaryKey"`
	Name     string `json:"name" gorm:"type:varchar(64);primaryKey;not null"`
	Addition string `json:"addition" gorm:"type:longtext" default:"{}"`
}
