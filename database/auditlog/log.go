package auditlog

import (
	"log"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func Log(ip, uuid, message, msgType string) {
	LogForUser("", ip, uuid, message, msgType)
}

func LogForUser(userID, ip, uuid, message, msgType string) {
	now := time.Now()
	if err := createLogEntryWithDB(dbcore.GetDBInstance(), strings.TrimSpace(userID), ip, uuid, message, msgType, models.FromTime(now)); err != nil {
		log.Println("Failed to create audit log entry:", err)
	}
}

func createLogEntryWithDB(db *gorm.DB, userID, ip, uuid, message, msgType string, timestamp models.LocalTime) error {
	logEntry := models.Log{
		UserID:  strings.TrimSpace(userID),
		IP:      ip,
		UUID:    uuid,
		Message: message,
		MsgType: msgType,
		Time:    timestamp,
	}
	return db.Create(&logEntry).Error
}

func EventLog(eventType, message string) {
	Log("", "", message, eventType)
}

func ListLogsByUser(userUUID string, limit, offset int) ([]models.Log, int64, error) {
	return listLogsByUserWithDB(dbcore.GetDBInstance(), strings.TrimSpace(userUUID), limit, offset)
}

func countLogsByUserWithDB(db *gorm.DB, userUUID string) (int64, error) {
	var total int64
	err := db.Model(&models.Log{}).
		Where("user_id = ?", userUUID).
		Count(&total).Error
	return total, err
}

func listLogsByUserWithDB(db *gorm.DB, userUUID string, limit, offset int) ([]models.Log, int64, error) {
	total, err := countLogsByUserWithDB(db, userUUID)
	if err != nil {
		return nil, 0, err
	}

	var logs []models.Log
	if err := db.Where("user_id = ?", userUUID).
		Order("time desc").
		Limit(limit).
		Offset(offset).
		Find(&logs).Error; err != nil {
		return nil, 0, err
	}
	return logs, total, nil
}

// Delete logs older than 30 days
func RemoveOldLogs() {
	db := dbcore.GetDBInstance()
	threshold := time.Now().AddDate(0, 0, -30)
	if err := db.Where("time < ?", threshold).Delete(&models.Log{}).Error; err != nil {
		log.Println("Failed to remove old logs:", err)
	}
}
