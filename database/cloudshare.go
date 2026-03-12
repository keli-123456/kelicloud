package database

import (
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
)

func GetCloudInstanceShare(provider, resourceType, resourceID string) (*models.CloudInstanceShare, error) {
	db := dbcore.GetDBInstance()
	var share models.CloudInstanceShare
	if err := db.Where(
		"provider = ? AND resource_type = ? AND resource_id = ?",
		strings.TrimSpace(provider),
		strings.TrimSpace(resourceType),
		strings.TrimSpace(resourceID),
	).First(&share).Error; err != nil {
		return nil, err
	}
	return &share, nil
}

func GetCloudInstanceShareByToken(token string) (*models.CloudInstanceShare, error) {
	db := dbcore.GetDBInstance()
	var share models.CloudInstanceShare
	if err := db.Where("share_token = ?", strings.TrimSpace(token)).First(&share).Error; err != nil {
		return nil, err
	}
	return &share, nil
}

func SaveCloudInstanceShare(share *models.CloudInstanceShare) error {
	db := dbcore.GetDBInstance()
	return db.Save(share).Error
}

func DeleteCloudInstanceShare(share *models.CloudInstanceShare) error {
	db := dbcore.GetDBInstance()
	return db.Delete(share).Error
}
