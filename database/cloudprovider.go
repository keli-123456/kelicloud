package database

import (
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func normalizeTenantScopeID(tenantID string) (string, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID != "" {
		return tenantID, nil
	}
	return GetDefaultTenantID()
}

func getCloudProviderConfigWithDB(db *gorm.DB, tenantID, name string) (*models.CloudProvider, error) {
	var config models.CloudProvider
	if err := db.Where("tenant_id = ? AND name = ?", tenantID, name).First(&config).Error; err != nil {
		return nil, err
	}
	return &config, nil
}

func GetCloudProviderConfigByTenantAndName(tenantID, name string) (*models.CloudProvider, error) {
	resolvedTenantID, err := normalizeTenantScopeID(tenantID)
	if err != nil {
		return nil, err
	}
	db := dbcore.GetDBInstance()
	return getCloudProviderConfigWithDB(db, resolvedTenantID, strings.TrimSpace(name))
}

func GetCloudProviderConfigByName(name string) (*models.CloudProvider, error) {
	return GetCloudProviderConfigByTenantAndName("", name)
}

func saveCloudProviderConfigWithDB(db *gorm.DB, config *models.CloudProvider) error {
	if config == nil {
		return nil
	}
	return db.Save(config).Error
}

func SaveCloudProviderConfigForTenant(config *models.CloudProvider) error {
	if config == nil {
		return nil
	}
	resolvedTenantID, err := normalizeTenantScopeID(config.TenantID)
	if err != nil {
		return err
	}
	config.TenantID = resolvedTenantID
	config.Name = strings.TrimSpace(config.Name)
	db := dbcore.GetDBInstance()
	return saveCloudProviderConfigWithDB(db, config)
}

func SaveCloudProviderConfig(config *models.CloudProvider) error {
	if config == nil {
		return nil
	}
	if strings.TrimSpace(config.TenantID) == "" {
		resolvedTenantID, err := normalizeTenantScopeID("")
		if err != nil {
			return err
		}
		config.TenantID = resolvedTenantID
	}
	return SaveCloudProviderConfigForTenant(config)
}
