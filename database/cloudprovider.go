package database

import (
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
)

func GetCloudProviderConfigByName(name string) (*models.CloudProvider, error) {
	db := dbcore.GetDBInstance()
	var config models.CloudProvider
	if err := db.Where("name = ?", name).First(&config).Error; err != nil {
		return nil, err
	}
	return &config, nil
}

func SaveCloudProviderConfig(config *models.CloudProvider) error {
	db := dbcore.GetDBInstance()
	return db.Save(config).Error
}
