package clipboard

import (
	"fmt"
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func normalizeClipboardUserID(userUUID string) (string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return "", fmt.Errorf("user id is required")
	}
	return userUUID, nil
}

func createClipboardWithDB(db *gorm.DB, cb *models.Clipboard) error {
	return db.Create(cb).Error
}

func getClipboardByIDForUserWithDB(db *gorm.DB, id int, userUUID string) (*models.Clipboard, error) {
	userUUID, err := normalizeClipboardUserID(userUUID)
	if err != nil {
		return nil, err
	}

	var cb models.Clipboard
	if err := db.Where("id = ? AND user_id = ?", id, userUUID).First(&cb).Error; err != nil {
		return nil, err
	}
	return &cb, nil
}

func updateClipboardFieldsForUserWithDB(db *gorm.DB, id int, userUUID string, fields map[string]interface{}) error {
	userUUID, err := normalizeClipboardUserID(userUUID)
	if err != nil {
		return err
	}

	delete(fields, "user_id")
	delete(fields, "id")
	result := db.Model(&models.Clipboard{}).Where("id = ? AND user_id = ?", id, userUUID).Updates(fields)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func deleteClipboardForUserWithDB(db *gorm.DB, id int, userUUID string) error {
	cb, err := getClipboardByIDForUserWithDB(db, id, userUUID)
	if err != nil {
		return err
	}
	return db.Delete(cb).Error
}

func deleteClipboardBatchForUserWithDB(db *gorm.DB, ids []int, userUUID string) error {
	userUUID, err := normalizeClipboardUserID(userUUID)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	return db.Where("user_id = ? AND id IN ?", userUUID, ids).Delete(&models.Clipboard{}).Error
}

func listClipboardByUserWithDB(db *gorm.DB, userUUID string) ([]models.Clipboard, error) {
	userUUID, err := normalizeClipboardUserID(userUUID)
	if err != nil {
		return nil, err
	}

	var list []models.Clipboard
	if err := db.Where("user_id = ?", userUUID).Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func CreateClipboard(cb *models.Clipboard) error {
	db := dbcore.GetDBInstance()
	return createClipboardWithDB(db, cb)
}

func CreateClipboardForUser(userUUID string, cb *models.Clipboard) error {
	userUUID, err := normalizeClipboardUserID(userUUID)
	if err != nil {
		return err
	}
	cb.UserID = userUUID
	return CreateClipboard(cb)
}

func GetClipboardByIDForUser(id int, userUUID string) (*models.Clipboard, error) {
	db := dbcore.GetDBInstance()
	return getClipboardByIDForUserWithDB(db, id, userUUID)
}

func UpdateClipboardFieldsForUser(id int, userUUID string, fields map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	return updateClipboardFieldsForUserWithDB(db, id, userUUID, fields)
}

func DeleteClipboardForUser(id int, userUUID string) error {
	db := dbcore.GetDBInstance()
	return deleteClipboardForUserWithDB(db, id, userUUID)
}

func DeleteClipboardBatchForUser(ids []int, userUUID string) error {
	db := dbcore.GetDBInstance()
	return deleteClipboardBatchForUserWithDB(db, ids, userUUID)
}

func ListClipboardByUser(userUUID string) ([]models.Clipboard, error) {
	db := dbcore.GetDBInstance()
	return listClipboardByUserWithDB(db, userUUID)
}
