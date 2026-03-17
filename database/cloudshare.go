package database

import (
	"errors"
	"strings"

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
