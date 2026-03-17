package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type UserConfigItem struct {
	UserUUID string `gorm:"primaryKey;column:user_uuid;type:varchar(36)"`
	Key      string `gorm:"primaryKey;column:key;type:varchar(191)"`
	Value    string `gorm:"column:value;type:text"`
}

func (UserConfigItem) TableName() string {
	return "user_configs"
}

var userScopedKeys = map[string]struct{}{
	AutoDiscoveryKeyKey:       {},
	ScriptDomainKey:           {},
	BaseScriptsURLKey:         {},
	SendIpAddrToGuestKey:      {},
	TempShareTokenKey:         {},
	TempShareTokenExpireAtKey: {},
}

func IsUserScopedKey(key string) bool {
	_, ok := userScopedKeys[key]
	return ok
}

func UserScopedKeys() []string {
	keys := make([]string, 0, len(userScopedKeys))
	for key := range userScopedKeys {
		keys = append(keys, key)
	}
	return keys
}

func GetAsForUser[T any](userUUID, key string, defaul ...any) (T, error) {
	var zero T

	defaults := map[string]any{key: nil}
	if len(defaul) > 0 {
		defaults[key] = defaul[0]
	}

	values, err := GetManyForUser(userUUID, defaults)
	if err != nil {
		return zero, err
	}

	value, ok := values[key]
	if !ok {
		return zero, gorm.ErrRecordNotFound
	}

	target := reflect.ValueOf(&zero).Elem()
	if err := convertAndSet(value, target); err != nil {
		return zero, err
	}
	return zero, nil
}

func GetManyForUser(userUUID string, keys map[string]any) (map[string]any, error) {
	result := make(map[string]any)
	if len(keys) == 0 {
		return result, nil
	}

	userUUID = strings.TrimSpace(userUUID)

	globalKeys := make([]string, 0, len(keys))
	userKeys := make([]string, 0, len(keys))
	for key := range keys {
		if IsUserScopedKey(key) {
			userKeys = append(userKeys, key)
			continue
		}
		globalKeys = append(globalKeys, key)
	}

	globalItems, err := findConfigItemsByKeys(globalKeys)
	if err != nil {
		return nil, err
	}
	userItems, err := findUserConfigItemsByKeys(userUUID, userKeys)
	if err != nil {
		return nil, err
	}

	var toInsertGlobal []ConfigItem
	var toInsertUser []UserConfigItem

	for key, def := range keys {
		if IsUserScopedKey(key) {
			if raw, ok := userItems[key]; ok {
				var parsed any
				if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
					result[key] = parsed
				}
				continue
			}
			if def == nil || userUUID == "" {
				continue
			}

			result[key] = def
			bytes, err := json.Marshal(def)
			if err != nil {
				return nil, fmt.Errorf("marshal key %s failed: %w", key, err)
			}
			toInsertUser = append(toInsertUser, UserConfigItem{
				UserUUID: userUUID,
				Key:      key,
				Value:    string(bytes),
			})
			continue
		}

		if raw, ok := globalItems[key]; ok {
			var parsed any
			if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
				result[key] = parsed
			}
			continue
		}
		if def == nil {
			continue
		}

		result[key] = def
		bytes, err := json.Marshal(def)
		if err != nil {
			return nil, fmt.Errorf("marshal key %s failed: %w", key, err)
		}
		toInsertGlobal = append(toInsertGlobal, ConfigItem{
			Key:   key,
			Value: string(bytes),
		})
	}

	if err := upsertConfigItems(toInsertGlobal); err != nil {
		return nil, err
	}
	if err := upsertUserConfigItems(toInsertUser); err != nil {
		return nil, err
	}

	return result, nil
}

func GetAllForUser(userUUID string) (map[string]any, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return GetAll()
	}

	if _, err := GetManyForUser(userUUID, map[string]any{}); err != nil {
		return nil, err
	}

	result, err := GetAll()
	if err != nil {
		return nil, err
	}
	for key := range userScopedKeys {
		delete(result, key)
	}

	userItems, err := findUserConfigItemsByKeys(userUUID, nil)
	if err != nil {
		return nil, err
	}
	for key, raw := range userItems {
		var parsed any
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			result[key] = parsed
		}
	}
	return result, nil
}

func SetForUser(userUUID, key string, value any) error {
	return SetManyForUser(userUUID, map[string]any{key: value})
}

func SetManyForUser(userUUID string, values map[string]any) error {
	if len(values) == 0 {
		return nil
	}

	userUUID = strings.TrimSpace(userUUID)

	globalValues := make(map[string]any)
	userValues := make(map[string]any)
	for key, value := range values {
		if IsUserScopedKey(key) {
			if userUUID == "" {
				return gorm.ErrRecordNotFound
			}
			userValues[key] = value
			continue
		}
		globalValues[key] = value
	}

	if len(globalValues) > 0 {
		if err := SetMany(globalValues); err != nil {
			return err
		}
	}

	if len(userValues) == 0 {
		return nil
	}

	oldValues, err := getUserConfigValues(userUUID, mapKeys(userValues))
	if err != nil {
		return err
	}

	items := make([]UserConfigItem, 0, len(userValues))
	for key, value := range userValues {
		bytes, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("marshal key %s failed: %w", key, err)
		}
		items = append(items, UserConfigItem{
			UserUUID: userUUID,
			Key:      key,
			Value:    string(bytes),
		})
	}

	if err := upsertUserConfigItems(items); err != nil {
		return err
	}

	newValues := make(map[string]any, len(userValues))
	for key, value := range userValues {
		newValues[key] = value
	}
	publishEvent(oldValues, newValues)
	return nil
}

func FindUserUUIDByConfigValue(key string, value any) (string, error) {
	if !IsUserScopedKey(key) {
		return "", errors.New("config key is not user scoped")
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	var matches []UserConfigItem
	if err := db.Where("key = ? AND value = ?", key, string(bytes)).Limit(2).Find(&matches).Error; err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", gorm.ErrRecordNotFound
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("multiple users matched key %s", key)
	}
	return matches[0].UserUUID, nil
}

func EnsureAutoDiscoveryKeyForUser(userUUID string) (string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return "", gorm.ErrRecordNotFound
	}

	value, err := GetAsForUser[string](userUUID, AutoDiscoveryKeyKey)
	if err == nil {
		value = strings.TrimSpace(value)
		if len(value) >= 12 {
			return value, nil
		}
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", err
	}

	value = utils.GenerateRandomString(24)
	if len(value) < 12 {
		return "", errors.New("failed to generate auto discovery key")
	}
	if err := SetForUser(userUUID, AutoDiscoveryKeyKey, value); err != nil {
		return "", err
	}
	return value, nil
}

func ResolveValidTempShareUserUUID(tempKey string, now time.Time) (string, bool, error) {
	tempKey = strings.TrimSpace(tempKey)
	if tempKey == "" {
		return "", false, nil
	}

	userUUID, err := FindUserUUIDByConfigValue(TempShareTokenKey, tempKey)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", false, nil
		}
		return "", false, err
	}

	expireAt, err := GetAsForUser[int64](userUUID, TempShareTokenExpireAtKey, int64(0))
	if err != nil {
		return "", false, err
	}
	if expireAt <= 0 || expireAt < now.Unix() {
		return "", false, nil
	}

	return userUUID, true, nil
}

func findUserConfigItemsByKeys(userUUID string, keys []string) (map[string]string, error) {
	items := make(map[string]string)
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return items, nil
	}

	query := db.Model(&UserConfigItem{}).Where("user_uuid = ?", userUUID)
	if len(keys) > 0 {
		query = query.Where(configKeyIn(keys))
	}

	var rows []UserConfigItem
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, item := range rows {
		items[item.Key] = item.Value
	}
	return items, nil
}

func upsertUserConfigItems(items []UserConfigItem) error {
	if len(items) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_uuid"}, {Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&items).Error
}

func getUserConfigValues(userUUID string, keys []string) (map[string]any, error) {
	rawItems, err := findUserConfigItemsByKeys(userUUID, keys)
	if err != nil {
		return nil, err
	}

	values := make(map[string]any, len(rawItems))
	for key, raw := range rawItems {
		var parsed any
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			values[key] = parsed
		}
	}
	return values, nil
}
