package database

import (
	"errors"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func GetThemeConfigurationByTenantAndShort(tenantID, short string) (*models.ThemeConfiguration, error) {
	db := dbcore.GetDBInstance()
	return getThemeConfigurationByTenantAndShortWithDB(db, tenantID, short)
}

func UpsertThemeConfigurationForTenant(tenantID, short, data string) error {
	db := dbcore.GetDBInstance()
	return upsertThemeConfigurationForTenantWithDB(db, tenantID, short, data)
}

func getThemeConfigurationByTenantAndShortWithDB(db *gorm.DB, tenantID, short string) (*models.ThemeConfiguration, error) {
	var themeCfg models.ThemeConfiguration
	if err := db.Where("tenant_id = ? AND short = ?", tenantID, short).First(&themeCfg).Error; err != nil {
		return nil, err
	}
	return &themeCfg, nil
}

func upsertThemeConfigurationForTenantWithDB(db *gorm.DB, tenantID, short, data string) error {
	if tenantID == "" || short == "" {
		return errors.New("tenant id and theme short are required")
	}

	var themeCfg models.ThemeConfiguration
	return db.Where("tenant_id = ? AND short = ?", tenantID, short).
		Assign(models.ThemeConfiguration{TenantID: tenantID, Short: short, Data: data}).
		FirstOrCreate(&themeCfg).Error
}
