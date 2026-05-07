package models

type BillingPlan struct {
	ID              uint        `json:"id" gorm:"primaryKey"`
	Code            string      `json:"code" gorm:"type:varchar(64);uniqueIndex;not null"`
	Name            string      `json:"name" gorm:"type:varchar(100);not null"`
	Description     string      `json:"description" gorm:"type:text"`
	PriceCents      int64       `json:"price_cents" gorm:"type:bigint;default:0"`
	Currency        string      `json:"currency" gorm:"type:varchar(12);default:'CNY'"`
	DurationDays    int         `json:"duration_days" gorm:"type:int;default:30"`
	ServerQuota     int         `json:"server_quota" gorm:"type:int;default:0"`
	AllowedFeatures StringArray `json:"allowed_features" gorm:"type:text"`
	SortOrder       int         `json:"sort_order" gorm:"type:int;default:0"`
	Active          bool        `json:"active" gorm:"default:true;index"`
	Public          bool        `json:"public" gorm:"default:true;index"`
	CreatedAt       LocalTime   `json:"created_at"`
	UpdatedAt       LocalTime   `json:"updated_at"`
}

type PaymentMethod struct {
	ID           uint      `json:"id" gorm:"primaryKey"`
	Code         string    `json:"code" gorm:"type:varchar(64);uniqueIndex;not null"`
	Name         string    `json:"name" gorm:"type:varchar(100);not null"`
	Type         string    `json:"type" gorm:"type:varchar(32);default:'manual'"`
	Instructions string    `json:"instructions" gorm:"type:text"`
	PaymentURL   string    `json:"payment_url" gorm:"type:text"`
	QRImageURL   string    `json:"qr_image_url" gorm:"type:text"`
	Enabled      bool      `json:"enabled" gorm:"default:true;index"`
	SortOrder    int       `json:"sort_order" gorm:"type:int;default:0"`
	CreatedAt    LocalTime `json:"created_at"`
	UpdatedAt    LocalTime `json:"updated_at"`
}

type BillingOrder struct {
	ID               uint        `json:"id" gorm:"primaryKey"`
	OrderNo          string      `json:"order_no" gorm:"type:varchar(64);uniqueIndex;not null"`
	UserUUID         string      `json:"user_uuid" gorm:"type:varchar(36);index;not null"`
	PlanID           uint        `json:"plan_id" gorm:"index;not null"`
	PaymentMethodID  uint        `json:"payment_method_id" gorm:"index"`
	Status           string      `json:"status" gorm:"type:varchar(24);index;default:'pending'"`
	PlanCode         string      `json:"plan_code" gorm:"type:varchar(64)"`
	PlanName         string      `json:"plan_name" gorm:"type:varchar(100)"`
	AmountCents      int64       `json:"amount_cents" gorm:"type:bigint;default:0"`
	Currency         string      `json:"currency" gorm:"type:varchar(12);default:'CNY'"`
	DurationDays     int         `json:"duration_days" gorm:"type:int;default:30"`
	ServerQuota      int         `json:"server_quota" gorm:"type:int;default:0"`
	AllowedFeatures  StringArray `json:"allowed_features" gorm:"type:text"`
	PaymentCode      string      `json:"payment_code" gorm:"type:varchar(64)"`
	PaymentName      string      `json:"payment_name" gorm:"type:varchar(100)"`
	PaymentReference string      `json:"payment_reference" gorm:"type:varchar(255)"`
	AdminNote        string      `json:"admin_note" gorm:"type:text"`
	PaidAt           *LocalTime  `json:"paid_at" gorm:"type:timestamp"`
	FulfilledAt      *LocalTime  `json:"fulfilled_at" gorm:"type:timestamp"`
	CreatedAt        LocalTime   `json:"created_at"`
	UpdatedAt        LocalTime   `json:"updated_at"`
}
