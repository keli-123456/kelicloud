package notification

import (
	"github.com/komari-monitor/komari/database"
	clientdb "github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/notifier"
	"gorm.io/gorm"
)

func AddLoadNotification(clients []string, name string, metric string, threshold float32, ratio float32, interval int) (uint, error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return 0, err
	}
	return AddLoadNotificationForTenant(tenantID, clients, name, metric, threshold, ratio, interval)
}

func AddLoadNotificationForTenant(tenantID string, clientUUIDs []string, name string, metric string, threshold float32, ratio float32, interval int) (uint, error) {
	normalizedClients, err := clientdb.NormalizeClientUUIDsForTenant(tenantID, clientUUIDs)
	if err != nil {
		return 0, err
	}
	db := dbcore.GetDBInstance()
	notification := models.LoadNotification{
		TenantID:  tenantID,
		Clients:   normalizedClients,
		Name:      name,
		Metric:    metric,
		Threshold: threshold,
		Ratio:     ratio,
		Interval:  interval,
	}
	if err := db.Create(&notification).Error; err != nil {
		return 0, err
	}

	return notification.Id, ReloadLoadNotificationSchedule()
}

func DeleteLoadNotification(id []uint) error {
	db := dbcore.GetDBInstance()
	result := db.Where("id IN ?", id).Delete(&models.LoadNotification{})
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return ReloadLoadNotificationSchedule()
}

func DeleteLoadNotificationForTenant(tenantID string, id []uint) error {
	db := dbcore.GetDBInstance()
	result := db.Where("tenant_id = ? AND id IN ?", tenantID, id).Delete(&models.LoadNotification{})
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return ReloadLoadNotificationSchedule()
}

func EditLoadNotification(notifications []*models.LoadNotification) error {
	db := dbcore.GetDBInstance()
	for _, notification := range notifications {
		result := db.Model(&models.LoadNotification{}).Where("id = ?", notification.Id).Updates(notification)
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
	}

	return ReloadLoadNotificationSchedule()
}

func EditLoadNotificationForTenant(tenantID string, notifications []*models.LoadNotification) error {
	db := dbcore.GetDBInstance()
	for _, notification := range notifications {
		normalizedClients, err := clientdb.NormalizeClientUUIDsForTenant(tenantID, notification.Clients)
		if err != nil {
			return err
		}
		notification.TenantID = tenantID
		notification.Clients = normalizedClients
		result := db.Model(&models.LoadNotification{}).
			Where("id = ? AND tenant_id = ?", notification.Id, tenantID).
			Updates(notification)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
	}

	return ReloadLoadNotificationSchedule()
}

func GetAllLoadNotifications() ([]models.LoadNotification, error) {
	db := dbcore.GetDBInstance()
	var notifications []models.LoadNotification
	if err := db.Find(&notifications).Error; err != nil {
		return nil, err
	}
	return notifications, nil
}

func GetAllLoadNotificationsByTenant(tenantID string) ([]models.LoadNotification, error) {
	db := dbcore.GetDBInstance()
	var notifications []models.LoadNotification
	if err := db.Where("tenant_id = ?", tenantID).Find(&notifications).Error; err != nil {
		return nil, err
	}
	return notifications, nil
}

func SaveLoadNotification(record models.LoadNotification) error {
	db := dbcore.GetDBInstance()
	return db.Create(&record).Error
}

func ReloadLoadNotificationSchedule() error {
	db := dbcore.GetDBInstance()
	var loadNotifications []models.LoadNotification
	if err := db.Find(&loadNotifications).Error; err != nil {
		return err
	}
	return notifier.ReloadLoadNotificationSchedule(loadNotifications)
}
