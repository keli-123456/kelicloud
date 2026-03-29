package clientddns

import (
	"errors"
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func normalizeUserID(userUUID string) (string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return "", errors.New("user id is required")
	}
	return userUUID, nil
}

func normalizeClientUUID(clientUUID string) (string, error) {
	clientUUID = strings.TrimSpace(clientUUID)
	if clientUUID == "" {
		return "", errors.New("client uuid is required")
	}
	return clientUUID, nil
}

func normalizeBinding(binding *models.ClientDDNSBinding, userUUID, clientUUID string) (*models.ClientDDNSBinding, error) {
	if binding == nil {
		return nil, errors.New("binding is required")
	}

	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return nil, err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return nil, err
	}

	next := *binding
	next.UserID = normalizedUserID
	next.ClientUUID = normalizedClientUUID
	next.Provider = strings.ToLower(strings.TrimSpace(next.Provider))
	next.EntryID = strings.TrimSpace(next.EntryID)
	next.AddressMode = strings.ToLower(strings.TrimSpace(next.AddressMode))
	next.Payload = strings.TrimSpace(next.Payload)
	next.RecordKey = strings.TrimSpace(next.RecordKey)
	next.LastIPv4 = strings.TrimSpace(next.LastIPv4)
	next.LastIPv6 = strings.TrimSpace(next.LastIPv6)
	next.LastError = strings.TrimSpace(next.LastError)
	next.LastResult = strings.TrimSpace(next.LastResult)
	return &next, nil
}

func getBindingByClientForUserWithDB(db *gorm.DB, userUUID, clientUUID string) (models.ClientDDNSBinding, error) {
	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return models.ClientDDNSBinding{}, err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return models.ClientDDNSBinding{}, err
	}

	var binding models.ClientDDNSBinding
	err = db.Where("user_id = ? AND client_uuid = ?", normalizedUserID, normalizedClientUUID).First(&binding).Error
	return binding, err
}

func saveBindingForUserWithDB(db *gorm.DB, userUUID, clientUUID string, binding *models.ClientDDNSBinding) (models.ClientDDNSBinding, error) {
	next, err := normalizeBinding(binding, userUUID, clientUUID)
	if err != nil {
		return models.ClientDDNSBinding{}, err
	}

	var existing models.ClientDDNSBinding
	if err := db.Where("user_id = ? AND client_uuid = ?", next.UserID, next.ClientUUID).First(&existing).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ClientDDNSBinding{}, err
		}
		if err := db.Create(next).Error; err != nil {
			return models.ClientDDNSBinding{}, err
		}
		return *next, nil
	}

	changed := existing.Enabled != next.Enabled ||
		existing.Provider != next.Provider ||
		existing.EntryID != next.EntryID ||
		existing.AddressMode != next.AddressMode ||
		existing.Payload != next.Payload ||
		existing.RecordKey != next.RecordKey

	existing.Enabled = next.Enabled
	existing.Provider = next.Provider
	existing.EntryID = next.EntryID
	existing.AddressMode = next.AddressMode
	existing.Payload = next.Payload
	existing.RecordKey = next.RecordKey
	if changed {
		existing.LastIPv4 = ""
		existing.LastIPv6 = ""
		existing.LastSyncedAt = nil
		existing.LastError = ""
		existing.LastResult = ""
	}
	if err := db.Save(&existing).Error; err != nil {
		return models.ClientDDNSBinding{}, err
	}
	return existing, nil
}

func deleteBindingForUserWithDB(db *gorm.DB, userUUID, clientUUID string) error {
	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return err
	}

	result := db.Where("user_id = ? AND client_uuid = ?", normalizedUserID, normalizedClientUUID).Delete(&models.ClientDDNSBinding{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func listEnabledBindingsWithDB(db *gorm.DB) ([]models.ClientDDNSBinding, error) {
	var bindings []models.ClientDDNSBinding
	if err := db.Where("enabled = ?", true).Find(&bindings).Error; err != nil {
		return nil, err
	}
	return bindings, nil
}

func findEnabledBindingByRecordKeyForUserWithDB(db *gorm.DB, userUUID, recordKey, excludedClientUUID string) (models.ClientDDNSBinding, error) {
	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return models.ClientDDNSBinding{}, err
	}
	recordKey = strings.TrimSpace(recordKey)
	if recordKey == "" {
		return models.ClientDDNSBinding{}, gorm.ErrRecordNotFound
	}

	query := db.Where("user_id = ? AND enabled = ? AND record_key = ?", normalizedUserID, true, recordKey)
	if excluded := strings.TrimSpace(excludedClientUUID); excluded != "" {
		query = query.Where("client_uuid <> ?", excluded)
	}

	var binding models.ClientDDNSBinding
	err = query.First(&binding).Error
	return binding, err
}

func updateBindingSyncStateByIDWithDB(
	db *gorm.DB,
	bindingID uint,
	lastIPv4 string,
	lastIPv6 string,
	lastSyncedAt *models.LocalTime,
	lastError string,
	lastResult string,
) error {
	updates := map[string]interface{}{
		"last_ipv4":      strings.TrimSpace(lastIPv4),
		"last_ipv6":      strings.TrimSpace(lastIPv6),
		"last_error":     strings.TrimSpace(lastError),
		"last_result":    strings.TrimSpace(lastResult),
		"last_synced_at": lastSyncedAt,
	}
	return db.Model(&models.ClientDDNSBinding{}).Where("id = ?", bindingID).Updates(updates).Error
}

func GetBindingByClientForUser(userUUID, clientUUID string) (models.ClientDDNSBinding, error) {
	return getBindingByClientForUserWithDB(dbcore.GetDBInstance(), userUUID, clientUUID)
}

func SaveBindingForUser(userUUID, clientUUID string, binding *models.ClientDDNSBinding) (models.ClientDDNSBinding, error) {
	return saveBindingForUserWithDB(dbcore.GetDBInstance(), userUUID, clientUUID, binding)
}

func DeleteBindingForUser(userUUID, clientUUID string) error {
	return deleteBindingForUserWithDB(dbcore.GetDBInstance(), userUUID, clientUUID)
}

func ListEnabledBindings() ([]models.ClientDDNSBinding, error) {
	return listEnabledBindingsWithDB(dbcore.GetDBInstance())
}

func FindEnabledBindingByRecordKeyForUser(userUUID, recordKey, excludedClientUUID string) (models.ClientDDNSBinding, error) {
	return findEnabledBindingByRecordKeyForUserWithDB(dbcore.GetDBInstance(), userUUID, recordKey, excludedClientUUID)
}

func UpdateBindingSyncStateByID(
	bindingID uint,
	lastIPv4 string,
	lastIPv6 string,
	lastSyncedAt *models.LocalTime,
	lastError string,
	lastResult string,
) error {
	return updateBindingSyncStateByIDWithDB(
		dbcore.GetDBInstance(),
		bindingID,
		lastIPv4,
		lastIPv6,
		lastSyncedAt,
		lastError,
		lastResult,
	)
}
