package notification

import (
	"strings"

	clientdb "github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/notifier"
	"gorm.io/gorm"
)

func normalizeLoadNotificationOwnerScope(userUUID, tenantID string) (string, string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID != "" {
		return userUUID, "", nil
	}
	return "", strings.TrimSpace(tenantID), nil
}

func AddLoadNotificationForUser(userUUID string, clientUUIDs []string, name string, metric string, threshold float32, ratio float32, interval int) (uint, error) {
	userUUID, _, err := normalizeLoadNotificationOwnerScope(userUUID, "")
	if err != nil {
		return 0, err
	}

	normalizedClients, err := clientdb.NormalizeClientUUIDsForUser(userUUID, clientUUIDs)
	if err != nil {
		return 0, err
	}
	db := dbcore.GetDBInstance()
	notification := models.LoadNotification{
		UserID:    userUUID,
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

func DeleteLoadNotificationForUser(userUUID string, id []uint) error {
	userUUID, _, err := normalizeLoadNotificationOwnerScope(userUUID, "")
	if err != nil {
		return err
	}

	db := dbcore.GetDBInstance()
	result := db.Where(
		"user_id = ? AND id IN ?",
		userUUID,
		id,
	).Delete(&models.LoadNotification{})
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

func EditLoadNotificationForUser(userUUID string, notifications []*models.LoadNotification) error {
	userUUID, _, err := normalizeLoadNotificationOwnerScope(userUUID, "")
	if err != nil {
		return err
	}

	db := dbcore.GetDBInstance()
	for _, notification := range notifications {
		normalizedClients, err := clientdb.NormalizeClientUUIDsForUser(userUUID, notification.Clients)
		if err != nil {
			return err
		}
		notification.UserID = userUUID
		notification.Clients = normalizedClients
		result := db.Model(&models.LoadNotification{}).
			Where("id = ? AND user_id = ?", notification.Id, userUUID).
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
	return getAllLoadNotificationsWithDB(dbcore.GetDBInstance())
}

func GetAllLoadNotificationsByUser(userUUID string) ([]models.LoadNotification, error) {
	return getAllLoadNotificationsByUserWithDB(dbcore.GetDBInstance(), userUUID)
}

func getAllLoadNotificationsWithDB(db *gorm.DB) ([]models.LoadNotification, error) {
	var notifications []models.LoadNotification
	if err := db.Find(&notifications).Error; err != nil {
		return nil, err
	}
	return notifications, nil
}

func getAllLoadNotificationsByUserWithDB(db *gorm.DB, userUUID string) ([]models.LoadNotification, error) {
	userUUID, _, err := normalizeLoadNotificationOwnerScope(userUUID, "")
	if err != nil {
		return nil, err
	}

	var notifications []models.LoadNotification
	if err := db.Where("user_id = ?", userUUID).Find(&notifications).Error; err != nil {
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
