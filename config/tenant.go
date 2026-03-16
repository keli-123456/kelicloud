package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TenantConfigItem struct {
	TenantID string `gorm:"primaryKey;column:tenant_id;type:varchar(36)"`
	Key      string `gorm:"primaryKey;column:key;type:varchar(191)"`
	Value    string `gorm:"column:value;type:text"`
}

func (TenantConfigItem) TableName() string {
	return "tenant_configs"
}

var tenantScopedKeys = map[string]struct{}{
	SitenameKey:               {},
	DescriptionKey:            {},
	ThemeKey:                  {},
	PrivateSiteKey:            {},
	AutoDiscoveryKeyKey:       {},
	ScriptDomainKey:           {},
	BaseScriptsURLKey:         {},
	SendIpAddrToGuestKey:      {},
	CustomHeadKey:             {},
	CustomBodyKey:             {},
	TempShareTokenKey:         {},
	TempShareTokenExpireAtKey: {},
}

func IsTenantScopedKey(key string) bool {
	_, ok := tenantScopedKeys[key]
	return ok
}

func BackfillTenantScopedConfigs(tenantID string) error {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || db == nil {
		return nil
	}

	keys := make([]string, 0, len(tenantScopedKeys))
	for key := range tenantScopedKeys {
		keys = append(keys, key)
	}

	var legacyItems []ConfigItem
	if err := db.Where(configKeyIn(keys)).Find(&legacyItems).Error; err != nil {
		return err
	}
	if len(legacyItems) == 0 {
		return nil
	}

	items := make([]TenantConfigItem, 0, len(legacyItems))
	for _, item := range legacyItems {
		items = append(items, TenantConfigItem{
			TenantID: tenantID,
			Key:      item.Key,
			Value:    item.Value,
		})
	}

	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "key"}},
		DoNothing: true,
	}).Create(&items).Error
}

func GetAsForTenant[T any](tenantID, key string, defaul ...any) (T, error) {
	var zero T

	defaults := map[string]any{key: nil}
	if len(defaul) > 0 {
		defaults[key] = defaul[0]
	}

	values, err := GetManyForTenant(tenantID, defaults)
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

func GetManyForTenant(tenantID string, keys map[string]any) (map[string]any, error) {
	result := make(map[string]any)
	if len(keys) == 0 {
		return result, nil
	}

	tenantID = strings.TrimSpace(tenantID)

	globalKeys := make([]string, 0, len(keys))
	tenantKeys := make([]string, 0, len(keys))
	for key := range keys {
		if tenantID != "" && IsTenantScopedKey(key) {
			tenantKeys = append(tenantKeys, key)
			continue
		}
		globalKeys = append(globalKeys, key)
	}

	globalItems, err := findConfigItemsByKeys(globalKeys)
	if err != nil {
		return nil, err
	}
	tenantItems, err := findTenantConfigItemsByKeys(tenantID, tenantKeys)
	if err != nil {
		return nil, err
	}

	var toInsertGlobal []ConfigItem
	var toInsertTenant []TenantConfigItem

	for key, def := range keys {
		if tenantID != "" && IsTenantScopedKey(key) {
			if raw, ok := tenantItems[key]; ok {
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
			toInsertTenant = append(toInsertTenant, TenantConfigItem{
				TenantID: tenantID,
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
	if err := upsertTenantConfigItems(toInsertTenant); err != nil {
		return nil, err
	}

	return result, nil
}

func GetManyAsForTenant[T any](tenantID string) (*T, error) {
	var t T
	val := reflect.ValueOf(&t).Elem()
	typ := val.Type()

	type fieldInfo struct {
		index int
		key   string
	}

	fields := make([]fieldInfo, 0)
	defaults := make(map[string]any)

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		key := strings.Split(jsonTag, ",")[0]
		if key == "" || key == "-" {
			continue
		}

		defaultVal, hasDefault := field.Tag.Lookup("default")
		if hasDefault {
			fieldValue := reflect.New(field.Type).Elem()
			if err := parseDefaultToField(defaultVal, fieldValue); err != nil {
				return nil, fmt.Errorf("parse default value failed for %s: %w", key, err)
			}
			defaults[key] = fieldValue.Interface()
		} else {
			defaults[key] = nil
		}

		fields = append(fields, fieldInfo{
			index: i,
			key:   key,
		})
	}

	values, err := GetManyForTenant(tenantID, defaults)
	if err != nil {
		return nil, err
	}

	for _, field := range fields {
		value, ok := values[field.key]
		if !ok {
			continue
		}
		if err := convertAndSet(value, val.Field(field.index)); err != nil {
			return nil, fmt.Errorf("convert config value failed for %s: %w", field.key, err)
		}
	}

	return &t, nil
}

func GetAllForTenant(tenantID string) (map[string]any, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return GetAll()
	}

	if _, err := GetManyAsForTenant[Legacy](tenantID); err != nil {
		return nil, err
	}

	result, err := GetAll()
	if err != nil {
		return nil, err
	}
	for key := range tenantScopedKeys {
		delete(result, key)
	}

	tenantItems, err := findTenantConfigItemsByKeys(tenantID, nil)
	if err != nil {
		return nil, err
	}
	for key, raw := range tenantItems {
		var parsed any
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			result[key] = parsed
		}
	}
	return result, nil
}

func SetForTenant(tenantID, key string, value any) error {
	return SetManyForTenant(tenantID, map[string]any{key: value})
}

func SetManyForTenant(tenantID string, cst map[string]any) error {
	if len(cst) == 0 {
		return nil
	}

	tenantID = strings.TrimSpace(tenantID)

	globalValues := make(map[string]any)
	tenantValues := make(map[string]any)
	for key, value := range cst {
		if tenantID != "" && IsTenantScopedKey(key) {
			tenantValues[key] = value
			continue
		}
		globalValues[key] = value
	}

	if len(globalValues) > 0 {
		if err := SetMany(globalValues); err != nil {
			return err
		}
	}

	if len(tenantValues) == 0 {
		return nil
	}

	oldValues, err := getTenantConfigValues(tenantID, mapKeys(tenantValues))
	if err != nil {
		return err
	}

	items := make([]TenantConfigItem, 0, len(tenantValues))
	for key, value := range tenantValues {
		bytes, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("marshal key %s failed: %w", key, err)
		}
		items = append(items, TenantConfigItem{
			TenantID: tenantID,
			Key:      key,
			Value:    string(bytes),
		})
	}

	if err := upsertTenantConfigItems(items); err != nil {
		return err
	}

	newValues := make(map[string]any, len(tenantValues))
	for key, value := range tenantValues {
		newValues[key] = value
	}
	publishEvent(oldValues, newValues)
	return nil
}

func FindTenantIDByConfigValue(key string, value any) (string, error) {
	if !IsTenantScopedKey(key) {
		return "", errors.New("config key is not tenant scoped")
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	var matches []TenantConfigItem
	if err := db.Where("key = ? AND value = ?", key, string(bytes)).Limit(2).Find(&matches).Error; err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", gorm.ErrRecordNotFound
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("multiple tenants matched key %s", key)
	}
	return matches[0].TenantID, nil
}

func findConfigItemsByKeys(keys []string) (map[string]string, error) {
	items := make(map[string]string)
	if len(keys) == 0 {
		return items, nil
	}

	var rows []ConfigItem
	if err := db.Where(configKeyIn(keys)).Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, item := range rows {
		items[item.Key] = item.Value
	}
	return items, nil
}

func findTenantConfigItemsByKeys(tenantID string, keys []string) (map[string]string, error) {
	items := make(map[string]string)
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return items, nil
	}

	query := db.Model(&TenantConfigItem{}).Where("tenant_id = ?", tenantID)
	if len(keys) > 0 {
		query = query.Where(configKeyIn(keys))
	}

	var rows []TenantConfigItem
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, item := range rows {
		items[item.Key] = item.Value
	}
	return items, nil
}

func upsertConfigItems(items []ConfigItem) error {
	if len(items) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&items).Error
}

func upsertTenantConfigItems(items []TenantConfigItem) error {
	if len(items) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&items).Error
}

func getTenantConfigValues(tenantID string, keys []string) (map[string]any, error) {
	rawItems, err := findTenantConfigItemsByKeys(tenantID, keys)
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

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}
