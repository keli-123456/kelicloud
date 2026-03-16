package auditlog

import (
	"log"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func Log(ip, uuid, message, msgType string) {
	LogForTenant("", ip, uuid, message, msgType)
}

func LogForTenant(tenantID, ip, uuid, message, msgType string) {
	now := time.Now()
	resolvedTenantID := normalizeLogTenantID(tenantID)
	if err := createLogEntryWithDB(dbcore.GetDBInstance(), resolvedTenantID, ip, uuid, message, msgType, models.FromTime(now)); err != nil {
		log.Println("Failed to create audit log entry:", err)
	}
}

func createLogEntryWithDB(db *gorm.DB, tenantID, ip, uuid, message, msgType string, timestamp models.LocalTime) error {
	logEntry := models.Log{
		TenantID: tenantID,
		IP:       ip,
		UUID:     uuid,
		Message:  message,
		MsgType:  msgType,
		Time:     timestamp,
	}
	return db.Create(&logEntry).Error
}

func EventLog(eventType, message string) {
	Log("", "", message, eventType)
}

func CountLogsByTenant(tenantID string) (int64, error) {
	return countLogsByTenantWithDB(dbcore.GetDBInstance(), normalizeLogTenantID(tenantID))
}

func ListLogsByTenant(tenantID string, limit, offset int) ([]models.Log, int64, error) {
	return listLogsByTenantWithDB(dbcore.GetDBInstance(), normalizeLogTenantID(tenantID), limit, offset)
}

func countLogsByTenantWithDB(db *gorm.DB, tenantID string) (int64, error) {
	var total int64
	err := db.Model(&models.Log{}).Where("tenant_id = ?", tenantID).Count(&total).Error
	return total, err
}

func listLogsByTenantWithDB(db *gorm.DB, tenantID string, limit, offset int) ([]models.Log, int64, error) {
	total, err := countLogsByTenantWithDB(db, tenantID)
	if err != nil {
		return nil, 0, err
	}

	var logs []models.Log
	if err := db.Where("tenant_id = ?", tenantID).Order("time desc").Limit(limit).Offset(offset).Find(&logs).Error; err != nil {
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

func normalizeLogTenantID(tenantID string) string {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID != "" {
		return tenantID
	}
	defaultTenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return ""
	}
	return defaultTenantID
}
