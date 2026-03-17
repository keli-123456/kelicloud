package models

type CloudProvider struct {
	UserID   string `json:"user_id,omitempty" gorm:"type:varchar(36);primaryKey"`
	Name     string `json:"name" gorm:"type:varchar(64);primaryKey;not null"`
	Addition string `json:"addition" gorm:"type:longtext" default:"{}"`
}
