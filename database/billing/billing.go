package billing

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

const (
	OrderStatusPending   = "pending"
	OrderStatusPaid      = "paid"
	OrderStatusFulfilled = "fulfilled"
	OrderStatusCancelled = "cancelled"
)

var ErrOrderClosed = errors.New("order is already closed")

func ListPlans(includeInactive bool) ([]models.BillingPlan, error) {
	db := dbcore.GetDBInstance()
	var plans []models.BillingPlan
	query := db.Model(&models.BillingPlan{})
	if !includeInactive {
		query = query.Where("active = ? AND public = ?", true, true)
	}
	err := query.Order("sort_order ASC").Order("id ASC").Find(&plans).Error
	return plans, err
}

func SavePlan(plan models.BillingPlan) (models.BillingPlan, error) {
	normalized, err := normalizePlan(plan)
	if err != nil {
		return models.BillingPlan{}, err
	}

	db := dbcore.GetDBInstance()
	if normalized.ID == 0 {
		if err := db.Create(&normalized).Error; err != nil {
			return models.BillingPlan{}, err
		}
		return normalized, nil
	}

	var existing models.BillingPlan
	if err := db.First(&existing, normalized.ID).Error; err != nil {
		return models.BillingPlan{}, err
	}
	if err := db.Model(&existing).Updates(map[string]any{
		"code":             normalized.Code,
		"name":             normalized.Name,
		"description":      normalized.Description,
		"price_cents":      normalized.PriceCents,
		"currency":         normalized.Currency,
		"duration_days":    normalized.DurationDays,
		"server_quota":     normalized.ServerQuota,
		"allowed_features": normalized.AllowedFeatures,
		"sort_order":       normalized.SortOrder,
		"active":           normalized.Active,
		"public":           normalized.Public,
	}).Error; err != nil {
		return models.BillingPlan{}, err
	}
	if err := db.First(&existing, normalized.ID).Error; err != nil {
		return models.BillingPlan{}, err
	}
	return existing, nil
}

func ArchivePlan(id uint) error {
	if id == 0 {
		return gorm.ErrRecordNotFound
	}
	return dbcore.GetDBInstance().
		Model(&models.BillingPlan{}).
		Where("id = ?", id).
		Updates(map[string]any{"active": false, "public": false}).Error
}

func ListPaymentMethods(includeDisabled bool) ([]models.PaymentMethod, error) {
	db := dbcore.GetDBInstance()
	var methods []models.PaymentMethod
	query := db.Model(&models.PaymentMethod{})
	if !includeDisabled {
		query = query.Where("enabled = ?", true)
	}
	err := query.Order("sort_order ASC").Order("id ASC").Find(&methods).Error
	return methods, err
}

func SavePaymentMethod(method models.PaymentMethod) (models.PaymentMethod, error) {
	normalized, err := normalizePaymentMethod(method)
	if err != nil {
		return models.PaymentMethod{}, err
	}

	db := dbcore.GetDBInstance()
	if normalized.ID == 0 {
		if err := db.Create(&normalized).Error; err != nil {
			return models.PaymentMethod{}, err
		}
		return normalized, nil
	}

	var existing models.PaymentMethod
	if err := db.First(&existing, normalized.ID).Error; err != nil {
		return models.PaymentMethod{}, err
	}
	if err := db.Model(&existing).Updates(map[string]any{
		"code":         normalized.Code,
		"name":         normalized.Name,
		"type":         normalized.Type,
		"instructions": normalized.Instructions,
		"payment_url":  normalized.PaymentURL,
		"qr_image_url": normalized.QRImageURL,
		"enabled":      normalized.Enabled,
		"sort_order":   normalized.SortOrder,
	}).Error; err != nil {
		return models.PaymentMethod{}, err
	}
	if err := db.First(&existing, normalized.ID).Error; err != nil {
		return models.PaymentMethod{}, err
	}
	return existing, nil
}

func DisablePaymentMethod(id uint) error {
	if id == 0 {
		return gorm.ErrRecordNotFound
	}
	return dbcore.GetDBInstance().
		Model(&models.PaymentMethod{}).
		Where("id = ?", id).
		Update("enabled", false).Error
}

func CreateOrder(userUUID string, planID, paymentMethodID uint) (models.BillingOrder, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return models.BillingOrder{}, gorm.ErrRecordNotFound
	}

	db := dbcore.GetDBInstance()
	var plan models.BillingPlan
	if err := db.Where("id = ? AND active = ? AND public = ?", planID, true, true).First(&plan).Error; err != nil {
		return models.BillingOrder{}, err
	}

	var method models.PaymentMethod
	if err := db.Where("id = ? AND enabled = ?", paymentMethodID, true).First(&method).Error; err != nil {
		return models.BillingOrder{}, err
	}

	order := models.BillingOrder{
		OrderNo:         newOrderNo(),
		UserUUID:        userUUID,
		PlanID:          plan.ID,
		PaymentMethodID: method.ID,
		Status:          OrderStatusPending,
		PlanCode:        plan.Code,
		PlanName:        plan.Name,
		AmountCents:     plan.PriceCents,
		Currency:        plan.Currency,
		DurationDays:    plan.DurationDays,
		ServerQuota:     plan.ServerQuota,
		AllowedFeatures: normalizedStringArray(plan.AllowedFeatures),
		PaymentCode:     method.Code,
		PaymentName:     method.Name,
	}
	if err := db.Create(&order).Error; err != nil {
		return models.BillingOrder{}, err
	}
	return order, nil
}

type OrderFilter struct {
	UserUUID string
	Status   string
	Limit    int
}

func ListOrders(filter OrderFilter) ([]models.BillingOrder, error) {
	db := dbcore.GetDBInstance()
	query := db.Model(&models.BillingOrder{})
	if userUUID := strings.TrimSpace(filter.UserUUID); userUUID != "" {
		query = query.Where("user_uuid = ?", userUUID)
	}
	if status := strings.TrimSpace(filter.Status); status != "" {
		query = query.Where("status = ?", status)
	}
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	var orders []models.BillingOrder
	err := query.Order("id DESC").Limit(limit).Find(&orders).Error
	return orders, err
}

func MarkOrderPaid(orderID uint, reference, note string) (models.BillingOrder, error) {
	db := dbcore.GetDBInstance()
	var order models.BillingOrder
	if err := db.First(&order, orderID).Error; err != nil {
		return models.BillingOrder{}, err
	}
	if order.Status == OrderStatusCancelled || order.Status == OrderStatusFulfilled {
		return models.BillingOrder{}, ErrOrderClosed
	}
	now := models.Now()
	order.Status = OrderStatusPaid
	order.PaymentReference = strings.TrimSpace(reference)
	order.AdminNote = strings.TrimSpace(note)
	order.PaidAt = &now

	if err := fulfillOrderPolicy(order); err != nil {
		return models.BillingOrder{}, err
	}

	fulfilledAt := models.Now()
	order.Status = OrderStatusFulfilled
	order.FulfilledAt = &fulfilledAt
	if err := db.Save(&order).Error; err != nil {
		return models.BillingOrder{}, err
	}
	return order, nil
}

func CancelOrder(orderID uint, note string) (models.BillingOrder, error) {
	db := dbcore.GetDBInstance()
	var order models.BillingOrder
	if err := db.First(&order, orderID).Error; err != nil {
		return models.BillingOrder{}, err
	}
	if order.Status == OrderStatusFulfilled {
		return models.BillingOrder{}, ErrOrderClosed
	}
	order.Status = OrderStatusCancelled
	if nextNote := strings.TrimSpace(note); nextNote != "" {
		order.AdminNote = nextNote
	}
	if err := db.Save(&order).Error; err != nil {
		return models.BillingOrder{}, err
	}
	return order, nil
}

func EnsureDefaultBillingRecords() error {
	db := dbcore.GetDBInstance()
	var planCount int64
	if err := db.Model(&models.BillingPlan{}).Count(&planCount).Error; err != nil {
		return err
	}
	if planCount == 0 {
		defaultPlans := []models.BillingPlan{
			{
				Code:            "starter",
				Name:            "Starter",
				Description:     "适合小规模自用服务器，包含基础监控、日志和历史记录。",
				PriceCents:      900,
				Currency:        "CNY",
				DurationDays:    30,
				ServerQuota:     3,
				AllowedFeatures: models.StringArray{"clients", "records", "logs"},
				SortOrder:       10,
				Active:          true,
				Public:          true,
			},
			{
				Code:         "ops",
				Name:         "Ops",
				Description:  "适合日常运维场景，开放脚本执行、通知和审计日志。",
				PriceCents:   2900,
				Currency:     "CNY",
				DurationDays: 30,
				ServerQuota:  10,
				AllowedFeatures: models.StringArray{
					"clients",
					"records",
					"tasks",
					"notifications",
					"clipboard",
					"logs",
				},
				SortOrder: 20,
				Active:    true,
				Public:    true,
			},
			{
				Code:            "business",
				Name:            "Business",
				Description:     "开放云厂商、DNS、故障切换和不限服务器额度。",
				PriceCents:      9900,
				Currency:        "CNY",
				DurationDays:    30,
				ServerQuota:     0,
				AllowedFeatures: models.StringArray(config.UserAvailableFeatures()),
				SortOrder:       30,
				Active:          true,
				Public:          true,
			},
		}
		for _, plan := range defaultPlans {
			if _, err := SavePlan(plan); err != nil {
				return err
			}
		}
	}

	var methodCount int64
	if err := db.Model(&models.PaymentMethod{}).Count(&methodCount).Error; err != nil {
		return err
	}
	if methodCount == 0 {
		_, err := SavePaymentMethod(models.PaymentMethod{
			Code:         "manual",
			Name:         "Manual payment",
			Type:         "manual",
			Instructions: "在这里填写转账账号、收款二维码链接或支付链接。收到付款后，在订单里标记已付款即可自动开通套餐。",
			Enabled:      true,
			SortOrder:    10,
		})
		return err
	}
	return nil
}

func normalizePlan(plan models.BillingPlan) (models.BillingPlan, error) {
	plan.Code = strings.ToLower(strings.TrimSpace(plan.Code))
	plan.Name = strings.TrimSpace(plan.Name)
	plan.Description = strings.TrimSpace(plan.Description)
	plan.Currency = strings.ToUpper(strings.TrimSpace(plan.Currency))
	if plan.Currency == "" {
		plan.Currency = "CNY"
	}
	if plan.Code == "" {
		return models.BillingPlan{}, fmt.Errorf("plan code is required")
	}
	if plan.Name == "" {
		return models.BillingPlan{}, fmt.Errorf("plan name is required")
	}
	if plan.PriceCents < 0 {
		return models.BillingPlan{}, fmt.Errorf("plan price must be greater than or equal to 0")
	}
	if plan.DurationDays < 0 {
		return models.BillingPlan{}, fmt.Errorf("plan duration must be greater than or equal to 0")
	}
	if plan.ServerQuota < 0 {
		return models.BillingPlan{}, fmt.Errorf("server quota must be greater than or equal to 0")
	}
	features := []string(plan.AllowedFeatures)
	if err := config.ValidateAllowedFeatures(features); err != nil {
		return models.BillingPlan{}, err
	}
	plan.AllowedFeatures = normalizedStringArray(config.NormalizeAllowedFeatures(features))
	return plan, nil
}

func normalizePaymentMethod(method models.PaymentMethod) (models.PaymentMethod, error) {
	method.Code = strings.ToLower(strings.TrimSpace(method.Code))
	method.Name = strings.TrimSpace(method.Name)
	method.Type = strings.ToLower(strings.TrimSpace(method.Type))
	method.Instructions = strings.TrimSpace(method.Instructions)
	method.PaymentURL = strings.TrimSpace(method.PaymentURL)
	method.QRImageURL = strings.TrimSpace(method.QRImageURL)
	if method.Type == "" {
		method.Type = "manual"
	}
	if method.Code == "" {
		return models.PaymentMethod{}, fmt.Errorf("payment method code is required")
	}
	if method.Name == "" {
		return models.PaymentMethod{}, fmt.Errorf("payment method name is required")
	}
	return method, nil
}

func fulfillOrderPolicy(order models.BillingOrder) error {
	features := []string(order.AllowedFeatures)
	if err := config.ValidateAllowedFeatures(features); err != nil {
		return err
	}
	features = config.NormalizeAllowedFeatures(features)

	expiresAt := ""
	if order.DurationDays > 0 {
		base := time.Now().In(models.GetAppLocation())
		policy, err := config.GetUserPolicy(order.UserUUID)
		if err == nil {
			if current, ok := parsePlanExpiration(policy.PlanExpiresAt); ok && current.After(base) {
				base = current
			}
		}
		expiresAt = base.AddDate(0, 0, order.DurationDays).Format("2006-01-02")
	}

	noteParts := []string{order.OrderNo}
	if order.PaymentReference != "" {
		noteParts = append(noteParts, order.PaymentReference)
	}
	if order.AdminNote != "" {
		noteParts = append(noteParts, order.AdminNote)
	}
	note := strings.Join(noteParts, " / ")
	disabled := false
	if err := config.SetUserPolicy(order.UserUUID, &order.ServerQuota, &features); err != nil {
		return err
	}
	return config.SetUserCommercialPolicy(order.UserUUID, &order.PlanName, &expiresAt, &note, &disabled)
}

func parsePlanExpiration(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	if parsed, err := time.ParseInLocation("2006-01-02", value, models.GetAppLocation()); err == nil {
		return parsed, true
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.In(models.GetAppLocation()), true
	}
	return time.Time{}, false
}

func normalizedStringArray(features []string) models.StringArray {
	output := make(models.StringArray, 0, len(features))
	for _, feature := range features {
		value := strings.TrimSpace(feature)
		if value != "" {
			output = append(output, value)
		}
	}
	return output
}

func newOrderNo() string {
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	if len(id) > 10 {
		id = id[:10]
	}
	return fmt.Sprintf("KC%s%s", time.Now().UTC().Format("20060102150405"), strings.ToUpper(id))
}
