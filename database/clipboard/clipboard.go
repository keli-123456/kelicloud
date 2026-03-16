package clipboard

import (
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

// CreateClipboard 创建剪贴板记录
func CreateClipboard(cb *models.Clipboard) error {
	db := dbcore.GetDBInstance()
	return db.Create(cb).Error
}

func CreateClipboardForTenant(tenantID string, cb *models.Clipboard) error {
	cb.TenantID = tenantID
	return CreateClipboard(cb)
}

// GetClipboardByID 根据ID获取剪贴板记录
func GetClipboardByID(id int) (*models.Clipboard, error) {
	var cb models.Clipboard
	db := dbcore.GetDBInstance()
	if err := db.First(&cb, id).Error; err != nil {
		return nil, err
	}
	return &cb, nil
}

func GetClipboardByIDForTenant(id int, tenantID string) (*models.Clipboard, error) {
	var cb models.Clipboard
	db := dbcore.GetDBInstance()
	if err := db.Where("id = ? AND tenant_id = ?", id, tenantID).First(&cb).Error; err != nil {
		return nil, err
	}
	return &cb, nil
}

// UpdateClipboardFields 更新剪贴板记录
func UpdateClipboardFields(id int, fields map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	return db.Model(&models.Clipboard{}).Where("id = ?", id).Updates(fields).Error
}

func UpdateClipboardFieldsForTenant(id int, tenantID string, fields map[string]interface{}) error {
	delete(fields, "tenant_id")
	delete(fields, "id")
	db := dbcore.GetDBInstance()
	result := db.Model(&models.Clipboard{}).Where("id = ? AND tenant_id = ?", id, tenantID).Updates(fields)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// DeleteClipboard 删除剪贴板记录
func DeleteClipboard(id int) error {
	db := dbcore.GetDBInstance()
	// Check if record exists first
	var cb models.Clipboard
	if err := db.First(&cb, id).Error; err != nil {
		return err // Record not found or other error
	}
	return db.Delete(&cb).Error
}

func DeleteClipboardForTenant(id int, tenantID string) error {
	db := dbcore.GetDBInstance()
	var cb models.Clipboard
	if err := db.Where("id = ? AND tenant_id = ?", id, tenantID).First(&cb).Error; err != nil {
		return err
	}
	return db.Delete(&cb).Error
}

// DeleteClipboardBatch 批量删除剪贴板记录
func DeleteClipboardBatch(ids []int) error {
	if len(ids) == 0 {
		return nil
	}
	db := dbcore.GetDBInstance()
	return db.Where("id IN ?", ids).Delete(&models.Clipboard{}).Error
}

func DeleteClipboardBatchForTenant(ids []int, tenantID string) error {
	if len(ids) == 0 {
		return nil
	}
	db := dbcore.GetDBInstance()
	return db.Where("tenant_id = ? AND id IN ?", tenantID, ids).Delete(&models.Clipboard{}).Error
}

// ListClipboard 列出所有剪贴板记录
func ListClipboard() ([]models.Clipboard, error) {
	var list []models.Clipboard
	db := dbcore.GetDBInstance()
	if err := db.Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func ListClipboardByTenant(tenantID string) ([]models.Clipboard, error) {
	var list []models.Clipboard
	db := dbcore.GetDBInstance()
	if err := db.Where("tenant_id = ?", tenantID).Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}
