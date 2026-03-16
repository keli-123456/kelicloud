package models

type Tenant struct {
	ID          string         `json:"id" gorm:"type:varchar(36);primaryKey"`
	Slug        string         `json:"slug" gorm:"type:varchar(64);uniqueIndex;not null"`
	Name        string         `json:"name" gorm:"type:varchar(100);not null"`
	Description string         `json:"description" gorm:"type:text"`
	IsDefault   bool           `json:"is_default" gorm:"default:false"`
	Members     []TenantMember `json:"members,omitempty" gorm:"foreignKey:TenantID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	CreatedAt   LocalTime      `json:"created_at"`
	UpdatedAt   LocalTime      `json:"updated_at"`
}

type TenantMember struct {
	TenantID  string    `json:"tenant_id" gorm:"type:varchar(36);primaryKey"`
	UserUUID  string    `json:"user_uuid" gorm:"type:varchar(36);primaryKey;index"`
	Role      string    `json:"role" gorm:"type:varchar(20);not null;default:'viewer'"`
	Tenant    Tenant    `json:"tenant,omitempty" gorm:"foreignKey:TenantID;references:ID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	User      User      `json:"user,omitempty" gorm:"foreignKey:UserUUID;references:UUID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	CreatedAt LocalTime `json:"created_at"`
	UpdatedAt LocalTime `json:"updated_at"`
}
