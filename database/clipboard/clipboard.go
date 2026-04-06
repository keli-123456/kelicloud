package clipboard

import (
	"fmt"
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

const clipboardLikeEscapeChar = '!'

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

func buildClipboardSearchTokens(rawSearch string) []string {
	return strings.Fields(strings.ToLower(strings.TrimSpace(rawSearch)))
}

func buildClipboardContainsPattern(token string) string {
	if token == "" {
		return "%"
	}

	var builder strings.Builder
	builder.Grow(len(token) + 2)
	builder.WriteByte('%')
	for _, char := range token {
		switch char {
		case clipboardLikeEscapeChar, '%', '_':
			builder.WriteByte(clipboardLikeEscapeChar)
		}
		builder.WriteRune(char)
	}
	builder.WriteByte('%')
	return builder.String()
}

func applyClipboardSearchQuery(query *gorm.DB, rawSearch string) *gorm.DB {
	for _, token := range buildClipboardSearchTokens(rawSearch) {
		pattern := buildClipboardContainsPattern(token)
		query = query.Where(
			`(
				LOWER(name) LIKE ? ESCAPE '!'
				OR LOWER(COALESCE(remark, '')) LIKE ? ESCAPE '!'
				OR LOWER(text) LIKE ? ESCAPE '!'
			)`,
			pattern,
			pattern,
			pattern,
		)
	}
	return query
}

func listClipboardByUserWithDB(db *gorm.DB, userUUID string) ([]models.Clipboard, error) {
	userUUID, err := normalizeClipboardUserID(userUUID)
	if err != nil {
		return nil, err
	}

	var list []models.Clipboard
	if err := db.
		Where("user_id = ?", userUUID).
		Order("weight DESC").
		Order("id DESC").
		Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func listClipboardPageByUserWithDB(db *gorm.DB, userUUID string, page, limit int, search string) ([]models.Clipboard, int64, error) {
	userUUID, err := normalizeClipboardUserID(userUUID)
	if err != nil {
		return nil, 0, err
	}

	if page <= 0 {
		page = 1
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	query := db.Model(&models.Clipboard{}).Where("user_id = ?", userUUID)
	query = applyClipboardSearchQuery(query, search)

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var list []models.Clipboard
	offset := (page - 1) * limit
	if err := query.
		Order("weight DESC").
		Order("id DESC").
		Limit(limit).
		Offset(offset).
		Find(&list).Error; err != nil {
		return nil, 0, err
	}

	return list, total, nil
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

func ListClipboardPageByUser(userUUID string, page, limit int) ([]models.Clipboard, int64, error) {
	db := dbcore.GetDBInstance()
	return listClipboardPageByUserWithDB(db, userUUID, page, limit, "")
}

func ListClipboardPageByUserWithSearch(userUUID string, page, limit int, search string) ([]models.Clipboard, int64, error) {
	db := dbcore.GetDBInstance()
	return listClipboardPageByUserWithDB(db, userUUID, page, limit, search)
}
