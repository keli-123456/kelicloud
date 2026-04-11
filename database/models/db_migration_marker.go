package models

type DBMigrationMarker struct {
	Key       string    `json:"key" gorm:"type:varchar(191);primaryKey"`
	AppliedAt LocalTime `json:"applied_at" gorm:"type:timestamp;not null"`
	CreatedAt LocalTime `json:"created_at"`
	UpdatedAt LocalTime `json:"updated_at"`
}

func (DBMigrationMarker) TableName() string {
	return "db_migration_markers"
}
