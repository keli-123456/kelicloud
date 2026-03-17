package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"gorm.io/gorm"
)

const (
	UserServerQuotaKey     = "server_quota"
	UserAllowedFeaturesKey = "allowed_features"
)

const (
	UserFeatureClients       = "clients"
	UserFeatureRecords       = "records"
	UserFeatureTasks         = "tasks"
	UserFeaturePing          = "ping"
	UserFeatureNotifications = "notifications"
	UserFeatureCloud         = "cloud"
	UserFeatureClipboard     = "clipboard"
	UserFeatureLogs          = "logs"
)

var userFeatureSet = map[string]struct{}{
	UserFeatureClients:       {},
	UserFeatureRecords:       {},
	UserFeatureTasks:         {},
	UserFeaturePing:          {},
	UserFeatureNotifications: {},
	UserFeatureCloud:         {},
	UserFeatureClipboard:     {},
	UserFeatureLogs:          {},
}

type UserPolicy struct {
	ServerQuota     int      `json:"server_quota"`
	AllowedFeatures []string `json:"allowed_features,omitempty"`
}

func UserAvailableFeatures() []string {
	features := make([]string, 0, len(userFeatureSet))
	for feature := range userFeatureSet {
		features = append(features, feature)
	}
	sort.Strings(features)
	return features
}

func IsSupportedUserFeature(feature string) bool {
	_, ok := userFeatureSet[strings.ToLower(strings.TrimSpace(feature))]
	return ok
}

func NormalizeAllowedFeatures(features []string) []string {
	normalized := make([]string, 0, len(features))
	seen := make(map[string]struct{}, len(features))
	for _, feature := range features {
		value := strings.ToLower(strings.TrimSpace(feature))
		if value == "" {
			continue
		}
		if _, ok := userFeatureSet[value]; !ok {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	sort.Strings(normalized)
	return normalized
}

func ValidateAllowedFeatures(features []string) error {
	for _, feature := range features {
		value := strings.ToLower(strings.TrimSpace(feature))
		if value == "" {
			continue
		}
		if _, ok := userFeatureSet[value]; !ok {
			return fmt.Errorf("invalid user feature: %s", feature)
		}
	}
	return nil
}

func GetUserPolicy(userUUID string) (UserPolicy, error) {
	policy := UserPolicy{}

	quota, err := GetAsForUser[int](userUUID, UserServerQuotaKey)
	switch {
	case err == nil:
		if quota > 0 {
			policy.ServerQuota = quota
		}
	case errors.Is(err, gorm.ErrRecordNotFound):
	default:
		return policy, err
	}

	features, err := GetAsForUser[[]string](userUUID, UserAllowedFeaturesKey)
	switch {
	case err == nil:
		policy.AllowedFeatures = NormalizeAllowedFeatures(features)
	case errors.Is(err, gorm.ErrRecordNotFound):
	default:
		return policy, err
	}

	return policy, nil
}

func SetUserPolicy(userUUID string, serverQuota *int, allowedFeatures *[]string) error {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return gorm.ErrRecordNotFound
	}

	values := make(map[string]any)
	if serverQuota != nil {
		if *serverQuota < 0 {
			return fmt.Errorf("server quota must be greater than or equal to 0")
		}
		values[UserServerQuotaKey] = *serverQuota
	}
	if allowedFeatures != nil {
		if err := ValidateAllowedFeatures(*allowedFeatures); err != nil {
			return err
		}
		values[UserAllowedFeaturesKey] = NormalizeAllowedFeatures(*allowedFeatures)
	}

	if len(values) == 0 {
		return nil
	}
	return SetManyForUser(userUUID, values)
}

func GetUserServerQuota(userUUID string) (int, error) {
	policy, err := GetUserPolicy(userUUID)
	if err != nil {
		return 0, err
	}
	return policy.ServerQuota, nil
}

func IsUserFeatureAllowed(userUUID, feature string) (bool, error) {
	value := strings.ToLower(strings.TrimSpace(feature))
	if value == "" {
		return false, fmt.Errorf("feature is required")
	}
	if _, ok := userFeatureSet[value]; !ok {
		return false, fmt.Errorf("invalid user feature: %s", feature)
	}

	policy, err := GetUserPolicy(userUUID)
	if err != nil {
		return false, err
	}
	if len(policy.AllowedFeatures) == 0 {
		return true, nil
	}
	for _, allowed := range policy.AllowedFeatures {
		if allowed == value {
			return true, nil
		}
	}
	return false, nil
}
