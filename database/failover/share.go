package failover

import (
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func getShareByTaskForUserWithDB(db *gorm.DB, userUUID string, taskID uint) (*models.FailoverShare, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}

	var share models.FailoverShare
	if err := db.Where("user_id = ? AND task_id = ?", userUUID, taskID).First(&share).Error; err != nil {
		return nil, err
	}
	return &share, nil
}

func GetShareByTaskForUser(userUUID string, taskID uint) (*models.FailoverShare, error) {
	return getShareByTaskForUserWithDB(dbcore.GetDBInstance(), userUUID, taskID)
}

func getShareByTokenWithDB(db *gorm.DB, token string) (*models.FailoverShare, error) {
	var share models.FailoverShare
	if err := db.Where("share_token = ?", strings.TrimSpace(token)).First(&share).Error; err != nil {
		return nil, err
	}
	return &share, nil
}

func GetShareByToken(token string) (*models.FailoverShare, error) {
	return getShareByTokenWithDB(dbcore.GetDBInstance(), token)
}

func saveShareWithDB(db *gorm.DB, share *models.FailoverShare) error {
	if share == nil {
		return nil
	}
	userUUID, err := normalizeFailoverUserID(share.UserID)
	if err != nil {
		return err
	}
	share.UserID = userUUID
	share.ShareToken = strings.TrimSpace(share.ShareToken)
	share.TaskName = strings.TrimSpace(share.TaskName)
	share.Title = strings.TrimSpace(share.Title)
	share.Note = strings.TrimSpace(share.Note)
	share.AccessPolicy = strings.TrimSpace(share.AccessPolicy)
	if share.AccessPolicy == "" {
		share.AccessPolicy = "public"
	}
	if share.AccessCount < 0 {
		share.AccessCount = 0
	}
	if share.ExpiresAt != nil && share.ExpiresAt.IsZero() {
		share.ExpiresAt = nil
	}
	if share.LastAccessedAt != nil && share.LastAccessedAt.IsZero() {
		share.LastAccessedAt = nil
	}
	if share.ConsumedAt != nil && share.ConsumedAt.IsZero() {
		share.ConsumedAt = nil
	}
	return db.Save(share).Error
}

func SaveShare(share *models.FailoverShare) error {
	return saveShareWithDB(dbcore.GetDBInstance(), share)
}

func DeleteShare(share *models.FailoverShare) error {
	return dbcore.GetDBInstance().Delete(share).Error
}

func recordShareAccessWithDB(db *gorm.DB, share *models.FailoverShare, consume bool, accessedAt time.Time) (bool, error) {
	if share == nil {
		return false, nil
	}
	if accessedAt.IsZero() {
		accessedAt = time.Now().UTC()
	} else {
		accessedAt = accessedAt.UTC()
	}

	updates := map[string]interface{}{
		"last_accessed_at": accessedAt,
		"access_count":     gorm.Expr("access_count + ?", 1),
	}
	query := db.Model(&models.FailoverShare{}).Where("id = ?", share.ID)
	if consume {
		query = query.Where("consumed_at IS NULL")
		updates["consumed_at"] = accessedAt
	}

	result := query.Updates(updates)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected == 0 {
		return false, nil
	}

	share.LastAccessedAt = &accessedAt
	if consume {
		share.ConsumedAt = &accessedAt
	}
	share.AccessCount++
	return true, nil
}

func RecordShareAccess(share *models.FailoverShare, consume bool, accessedAt time.Time) (bool, error) {
	return recordShareAccessWithDB(dbcore.GetDBInstance(), share, consume, accessedAt)
}
