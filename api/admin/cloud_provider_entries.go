package admin

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/cloudprovider/factory"
)

const defaultCloudProviderEntryName = "Default"

type cloudProviderEntry struct {
	ID     string                 `json:"id"`
	Name   string                 `json:"name"`
	Values map[string]interface{} `json:"values"`
}

type cloudProviderEntriesDocument struct {
	Entries []cloudProviderEntry `json:"entries"`
}

type cloudProviderResponse struct {
	Name    string               `json:"name"`
	Entries []cloudProviderEntry `json:"entries"`
}

type setCloudProviderPayload struct {
	Addition string               `json:"addition"`
	Entries  []cloudProviderEntry `json:"entries"`
}

func buildCloudProviderResponse(providerName string, config *models.CloudProvider) (*cloudProviderResponse, error) {
	entries := []cloudProviderEntry{}
	if config != nil {
		parsedEntries, err := parseCloudProviderEntries(config.Addition)
		if err != nil {
			return nil, err
		}
		entries = parsedEntries
	}

	return &cloudProviderResponse{
		Name:    providerName,
		Entries: entries,
	}, nil
}

func parseCloudProviderEntries(addition string) ([]cloudProviderEntry, error) {
	trimmed := strings.TrimSpace(addition)
	if trimmed == "" || trimmed == "{}" || trimmed == "null" {
		return []cloudProviderEntry{}, nil
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &object); err == nil {
		if rawEntries, ok := object["entries"]; ok {
			if len(rawEntries) == 0 || string(rawEntries) == "null" {
				return []cloudProviderEntry{}, nil
			}

			var entries []cloudProviderEntry
			if err := json.Unmarshal(rawEntries, &entries); err != nil {
				return nil, fmt.Errorf("invalid cloud provider entries: %w", err)
			}
			return normalizeCloudProviderEntries(entries), nil
		}
	}

	var entries []cloudProviderEntry
	if err := json.Unmarshal([]byte(trimmed), &entries); err == nil {
		return normalizeCloudProviderEntries(entries), nil
	}

	legacyValues, err := parseLegacyCloudProviderValues(trimmed)
	if err != nil {
		return nil, err
	}
	if len(legacyValues) == 0 {
		return []cloudProviderEntry{}, nil
	}

	return []cloudProviderEntry{normalizeCloudProviderEntry(cloudProviderEntry{
		ID:     "default",
		Name:   defaultCloudProviderEntryName,
		Values: legacyValues,
	})}, nil
}

func parseLegacyCloudProviderValues(addition string) (map[string]interface{}, error) {
	trimmed := strings.TrimSpace(addition)
	if trimmed == "" || trimmed == "{}" || trimmed == "null" {
		return map[string]interface{}{}, nil
	}

	var values map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &values); err != nil {
		return nil, fmt.Errorf("invalid cloud provider configuration: %w", err)
	}

	if values == nil {
		return map[string]interface{}{}, nil
	}
	return values, nil
}

func normalizeCloudProviderEntries(entries []cloudProviderEntry) []cloudProviderEntry {
	normalized := make([]cloudProviderEntry, 0, len(entries))
	for _, entry := range entries {
		normalized = append(normalized, normalizeCloudProviderEntry(entry))
	}
	return normalized
}

func normalizeCloudProviderEntry(entry cloudProviderEntry) cloudProviderEntry {
	entry.ID = strings.TrimSpace(entry.ID)
	entry.Name = strings.TrimSpace(entry.Name)
	if entry.ID == "" {
		entry.ID = uuid.NewString()
	}
	if entry.Name == "" {
		entry.Name = defaultCloudProviderEntryName
	}
	if entry.Values == nil {
		entry.Values = map[string]interface{}{}
	}
	return entry
}

func validateCloudProviderEntries(providerName string, entries []cloudProviderEntry) ([]cloudProviderEntry, error) {
	constructor, exists := factory.GetConstructor(providerName)
	if !exists {
		return nil, fmt.Errorf("cloud provider not found: %s", providerName)
	}

	normalized := make([]cloudProviderEntry, 0, len(entries))
	seenIDs := make(map[string]struct{}, len(entries))

	for index, entry := range entries {
		entry.ID = strings.TrimSpace(entry.ID)
		entry.Name = strings.TrimSpace(entry.Name)
		if entry.Name == "" {
			return nil, fmt.Errorf("entry %d name is required", index+1)
		}
		if entry.ID == "" {
			entry.ID = uuid.NewString()
		}
		if _, exists := seenIDs[entry.ID]; exists {
			return nil, fmt.Errorf("duplicate entry id: %s", entry.ID)
		}
		seenIDs[entry.ID] = struct{}{}
		if entry.Values == nil {
			entry.Values = map[string]interface{}{}
		}

		configPayload, err := json.Marshal(entry.Values)
		if err != nil {
			return nil, fmt.Errorf("failed to encode cloud provider entry %q: %w", entry.Name, err)
		}

		provider := constructor()
		if err := json.Unmarshal(configPayload, provider.GetConfiguration()); err != nil {
			return nil, fmt.Errorf("invalid cloud provider entry %q: %w", entry.Name, err)
		}

		normalized = append(normalized, entry)
	}

	return normalized, nil
}

func convertLegacyAdditionToCloudProviderEntries(addition string) ([]cloudProviderEntry, error) {
	legacyValues, err := parseLegacyCloudProviderValues(addition)
	if err != nil {
		return nil, err
	}
	if len(legacyValues) == 0 {
		return []cloudProviderEntry{}, nil
	}

	return []cloudProviderEntry{{
		ID:     "default",
		Name:   defaultCloudProviderEntryName,
		Values: legacyValues,
	}}, nil
}

func marshalCloudProviderEntries(entries []cloudProviderEntry) (string, error) {
	document := cloudProviderEntriesDocument{Entries: normalizeCloudProviderEntries(entries)}
	payload, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}
