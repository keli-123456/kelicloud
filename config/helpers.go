package config

import (
	"gorm.io/gorm/clause"
)

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

func upsertConfigItems(items []ConfigItem) error {
	if len(items) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&items).Error
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}
