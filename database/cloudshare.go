package database

import (
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func GetCloudInstanceShare(provider, resourceType, resourceID string) (*models.CloudInstanceShare, error) {
	return GetCloudInstanceShareByTenant("", provider, resourceType, resourceID)
}

func getCloudInstanceShareWithDB(db *gorm.DB, tenantID, provider, resourceType, resourceID string) (*models.CloudInstanceShare, error) {
	var share models.CloudInstanceShare
	if err := db.Where(
		"tenant_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?",
		tenantID,
		strings.TrimSpace(provider),
		strings.TrimSpace(resourceType),
		strings.TrimSpace(resourceID),
	).First(&share).Error; err != nil {
		return nil, err
	}
	return &share, nil
}

func GetCloudInstanceShareByTenant(tenantID, provider, resourceType, resourceID string) (*models.CloudInstanceShare, error) {
	resolvedTenantID, err := normalizeTenantScopeID(tenantID)
	if err != nil {
		return nil, err
	}
	db := dbcore.GetDBInstance()
	return getCloudInstanceShareWithDB(db, resolvedTenantID, provider, resourceType, resourceID)
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
	resolvedTenantID, err := normalizeTenantScopeID(share.TenantID)
	if err != nil {
		return err
	}
	share.TenantID = resolvedTenantID
	share.ShareToken = strings.TrimSpace(share.ShareToken)
	share.Provider = strings.TrimSpace(share.Provider)
	share.ResourceType = strings.TrimSpace(share.ResourceType)
	share.ResourceID = strings.TrimSpace(share.ResourceID)
	return db.Save(share).Error
}

func DeleteCloudInstanceShare(share *models.CloudInstanceShare) error {
	db := dbcore.GetDBInstance()
	return db.Delete(share).Error
}
