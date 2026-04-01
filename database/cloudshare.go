package database

import (
	"errors"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func normalizeCloudShareUserID(userID string) (string, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "", errors.New("user id is required")
	}
	return userID, nil
}

func getCloudInstanceShareByUserWithDB(db *gorm.DB, userID, provider, resourceType, resourceID string) (*models.CloudInstanceShare, error) {
	userID, err := normalizeCloudShareUserID(userID)
	if err != nil {
		return nil, err
	}

	var share models.CloudInstanceShare
	query := db.Where(
		"user_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?",
		userID,
		strings.TrimSpace(provider),
		strings.TrimSpace(resourceType),
		strings.TrimSpace(resourceID),
	)
	if err := query.First(&share).Error; err != nil {
		return nil, err
	}
	return &share, nil
}

func GetCloudInstanceShareByUser(userID, provider, resourceType, resourceID string) (*models.CloudInstanceShare, error) {
	db := dbcore.GetDBInstance()
	return getCloudInstanceShareByUserWithDB(db, userID, provider, resourceType, resourceID)
}

func GetCloudInstanceShareByToken(token string) (*models.CloudInstanceShare, error) {
	db := dbcore.GetDBInstance()
	return getCloudInstanceShareByTokenWithDB(db, token)
}

func getCloudInstanceShareByTokenWithDB(db *gorm.DB, token string) (*models.CloudInstanceShare, error) {
	var share models.CloudInstanceShare
	if err := db.Where("share_token = ?", strings.TrimSpace(token)).First(&share).Error; err != nil {
		return nil, err
	}
	return &share, nil
}

func SaveCloudInstanceShare(share *models.CloudInstanceShare) error {
	db := dbcore.GetDBInstance()
	return saveCloudInstanceShareWithDB(db, share)
}

func saveCloudInstanceShareWithDB(db *gorm.DB, share *models.CloudInstanceShare) error {
	if share == nil {
		return nil
	}
	userID, err := normalizeCloudShareUserID(share.UserID)
	if err != nil {
		return err
	}
	share.UserID = userID
	share.ShareToken = strings.TrimSpace(share.ShareToken)
	share.Provider = strings.TrimSpace(share.Provider)
	share.ResourceType = strings.TrimSpace(share.ResourceType)
	share.ResourceID = strings.TrimSpace(share.ResourceID)
	share.ResourceName = strings.TrimSpace(share.ResourceName)
	share.CredentialID = strings.TrimSpace(share.CredentialID)
	share.Region = strings.TrimSpace(share.Region)
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

func SaveCloudInstanceShareForUser(share *models.CloudInstanceShare) error {
	db := dbcore.GetDBInstance()
	return saveCloudInstanceShareWithDB(db, share)
}

func DeleteCloudInstanceShare(share *models.CloudInstanceShare) error {
	db := dbcore.GetDBInstance()
	return db.Delete(share).Error
}

func RecordCloudInstanceShareAccess(share *models.CloudInstanceShare, consume bool, accessedAt time.Time) (bool, error) {
	db := dbcore.GetDBInstance()
	return recordCloudInstanceShareAccessWithDB(db, share, consume, accessedAt)
}

func recordCloudInstanceShareAccessWithDB(db *gorm.DB, share *models.CloudInstanceShare, consume bool, accessedAt time.Time) (bool, error) {
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
	query := db.Model(&models.CloudInstanceShare{})
	query = query.Where("id = ?", share.ID)
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
