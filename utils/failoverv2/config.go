package failoverv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/models"
)

const defaultProviderEntryID = "default"

type genericProviderEntry struct {
	ID     string                 `json:"id"`
	Name   string                 `json:"name"`
	Values map[string]interface{} `json:"values"`
}

type aliyunDNSConfig struct {
	AccessKeyID     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	RegionID        string `json:"region_id"`
	DomainName      string `json:"domain_name"`
}

var loadAliyunDNSConfigFunc = loadAliyunDNSConfig

func loadProviderAddition(userUUID, providerName string) (string, error) {
	providerConfig, err := database.GetCloudProviderConfigByUserAndName(strings.TrimSpace(userUUID), providerName)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(providerConfig.Addition), nil
}

func loadGenericProviderEntry(userUUID, providerName, entryID string) (*genericProviderEntry, error) {
	entryID = strings.TrimSpace(entryID)
	if entryID == "" {
		entryID = defaultProviderEntryID
	}

	raw, err := loadProviderAddition(userUUID, providerName)
	if err != nil {
		return nil, fmt.Errorf("%s provider is not configured", providerName)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" || raw == "null" {
		return nil, fmt.Errorf("%s provider is not configured", providerName)
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &object); err == nil {
		if rawEntries, ok := object["entries"]; ok {
			var entries []genericProviderEntry
			if err := json.Unmarshal(rawEntries, &entries); err != nil {
				return nil, fmt.Errorf("%s provider configuration is invalid: %w", providerName, err)
			}
			for _, entry := range entries {
				normalized := normalizeGenericProviderEntry(entry)
				if normalized.ID == entryID {
					return &normalized, nil
				}
			}
			return nil, fmt.Errorf("%s provider entry not found: %s", providerName, entryID)
		}
	}

	if entryID != defaultProviderEntryID {
		return nil, fmt.Errorf("%s provider entry not found: %s", providerName, entryID)
	}

	var values map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, fmt.Errorf("%s provider configuration is invalid: %w", providerName, err)
	}

	entry := normalizeGenericProviderEntry(genericProviderEntry{
		ID:     defaultProviderEntryID,
		Name:   "Default",
		Values: values,
	})
	return &entry, nil
}

func normalizeGenericProviderEntry(entry genericProviderEntry) genericProviderEntry {
	entry.ID = strings.TrimSpace(entry.ID)
	entry.Name = strings.TrimSpace(entry.Name)
	if entry.ID == "" {
		entry.ID = defaultProviderEntryID
	}
	if entry.Name == "" {
		entry.Name = "Default"
	}
	if entry.Values == nil {
		entry.Values = map[string]interface{}{}
	}
	return entry
}

func decodeGenericEntryConfig[T any](entry *genericProviderEntry) (*T, error) {
	if entry == nil {
		return nil, errors.New("provider entry is required")
	}
	payload, err := json.Marshal(entry.Values)
	if err != nil {
		return nil, err
	}

	var value T
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func loadAliyunDNSConfig(userUUID, entryID string) (*aliyunDNSConfig, error) {
	entry, err := loadGenericProviderEntry(userUUID, models.FailoverDNSProviderAliyun, entryID)
	if err != nil {
		return nil, err
	}

	configValue, err := decodeGenericEntryConfig[aliyunDNSConfig](entry)
	if err != nil {
		return nil, fmt.Errorf("aliyun dns config is invalid: %w", err)
	}
	if strings.TrimSpace(configValue.AccessKeyID) == "" || strings.TrimSpace(configValue.AccessKeySecret) == "" {
		return nil, errors.New("aliyun access_key_id and access_key_secret are required")
	}
	return configValue, nil
}
