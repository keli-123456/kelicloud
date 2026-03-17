package database

import (
	"errors"
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func normalizeCloudUserID(userID string) (string, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "", errors.New("user id is required")
	}
	return userID, nil
}

func getCloudProviderConfigByUserWithDB(db *gorm.DB, userID, name string) (*models.CloudProvider, error) {
	userID, err := normalizeCloudUserID(userID)
	if err != nil {
		return nil, err
	}

	var config models.CloudProvider
	query := db.Where("user_id = ? AND name = ?", userID, strings.TrimSpace(name))
	if err := query.First(&config).Error; err != nil {
		return nil, err
	}
	return &config, nil
}

func GetCloudProviderConfigByUserAndName(userID, name string) (*models.CloudProvider, error) {
	db := dbcore.GetDBInstance()
	return getCloudProviderConfigByUserWithDB(db, userID, name)
}

func saveCloudProviderConfigWithDB(db *gorm.DB, config *models.CloudProvider) error {
	if config == nil {
		return nil
	}
	return db.Save(config).Error
}

func saveCloudProviderConfigForUserWithDB(db *gorm.DB, config *models.CloudProvider) error {
	if config == nil {
		return nil
	}
	userID, err := normalizeCloudUserID(config.UserID)
	if err != nil {
		return err
	}
	config.UserID = userID
	config.Name = strings.TrimSpace(config.Name)
	result := db.Model(&models.CloudProvider{}).
		Where("user_id = ? AND name = ?", config.UserID, config.Name).
		Updates(map[string]interface{}{
			"addition": config.Addition,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return nil
	}
	return saveCloudProviderConfigWithDB(db, config)
}

func SaveCloudProviderConfigForUser(config *models.CloudProvider) error {
	return saveCloudProviderConfigForUserWithDB(dbcore.GetDBInstance(), config)
}
